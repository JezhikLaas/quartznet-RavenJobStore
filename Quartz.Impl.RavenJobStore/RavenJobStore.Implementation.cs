using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Quartz.Impl.Matchers;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Simpl;
using Quartz.Spi;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;

// ReSharper disable MemberCanBePrivate.Global
// Internal instead of private for unit tests.

[assembly: InternalsVisibleTo("Quartz.Impl.UnitTests")]

namespace Quartz.Impl.RavenJobStore;

public partial class RavenJobStore
{
    internal static RavenJobStore? Instance;

    public RavenJobStore()
    {
        Instance = this;
    }

    public RavenJobStore(IDocumentStore store)
    {
        Instance = this;
        DocumentStore = store;
    }

    internal IDocumentStore InitializeDocumentStore()
    {
        var store = new DocumentStore
        {
            Urls = RavenNodes,
            Database = Database,
            Certificate = string.IsNullOrEmpty(CertificatePath)
                ? null
                : new X509Certificate2(CertificatePath, CertificatePath)
        };

        store.Initialize();

        return store;
    }
    
    internal async Task SchedulerStartedAsync(CancellationToken token)
    {
        TraceEnter(Logger);
        
        Logger.LogDebug("Scheduler started at {PointInTime}", SystemTime.UtcNow());
        
        using var session = GetSession();

        var exists = await session.Advanced.ExistsAsync(InstanceName, token);

        if (exists == false)
        {
            var scheduler = new Scheduler { InstanceName = InstanceName, State = SchedulerState.Started };
            await session
                .StoreAsync(scheduler, InstanceName, token)
                .ConfigureAwait(false);
            await session
                .SaveChangesAsync(token)
                .ConfigureAwait(false);

            TraceExit(Logger);
            return;
        }

        // Scheduler with same instance name already exists, recover persistent data
        try
        {
            await RecoverJobStoreAsync(session, token).ConfigureAwait(false);
            TraceExit(Logger);
        }
        catch (SchedulerException error)
        {
            throw new SchedulerConfigException("Failure occurred during job recovery.", error);
        }
    }

    internal async Task SetSchedulerStateAsync(SchedulerState state, CancellationToken cancellationToken)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();
        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, cancellationToken)
            .ConfigureAwait(false);

        scheduler.ThrowIfNull().State = state;

        await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

        TraceExit(Logger);
    }
    
    internal async Task StoreJobAndTriggerAsync(
        IJobDetail newJob,
        IOperableTrigger newTrigger,
        CancellationToken cancellationToken)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();
        var jobToStore = new Job(newJob, InstanceName);

        var triggerToStore = await CreateConfiguredTriggerAsync
        (
            newTrigger,
            session, cancellationToken).ConfigureAwait(false);
        
        await session
            .StoreAsync(triggerToStore, triggerToStore.Id, cancellationToken)
            .ConfigureAwait(false);

        await session
            .StoreAsync(jobToStore, jobToStore.Id, cancellationToken)
            .ConfigureAwait(false);

        await session
            .SaveChangesAsync(cancellationToken)
            .ConfigureAwait(false);

        TraceExit(Logger);
    }

    internal async Task<bool> IsJobGroupPausedAsync(string groupName, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var result = await IsJobGroupPausedAsync(session, groupName, token).ConfigureAwait(false);

        TraceExit(Logger, result);
        
        return result;
    }

    internal async Task<bool> IsTriggerGroupPausedAsync(string groupName, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();
        var result =  await IsTriggerGroupPausedAsync(session, groupName, token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task StoreJobAsync(IJobDetail newJob, bool replaceExisting, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        if (await session.Advanced.ExistsAsync(newJob.Key.GetDatabaseId(InstanceName), token).ConfigureAwait(false))
        {
            if (replaceExisting == false)
            {
                TraceExit(Logger, nameof(ObjectAlreadyExistsException));
                throw new ObjectAlreadyExistsException(newJob);
            }
        }

        var job = new Job(newJob, InstanceName);

        await session.StoreAsync(job, job.Id, token).ConfigureAwait(false);
        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task StoreJobsAndTriggersAsync(
        IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
        bool replace,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggerIdsToAdd = triggersAndJobs
            .SelectMany(x => x.Value.Select(t => t.Key.GetDatabaseId(InstanceName)))
            .ToList();

        if (replace == false)
        {
            var triggerExists = await (
                from trigger in session.Query<Trigger>()
                where trigger.Id.In(triggerIdsToAdd)
                select trigger
            ).AnyAsync(token).ConfigureAwait(false);

            if (triggerExists)
            {
                TraceExit(Logger, nameof(ObjectAlreadyExistsException));
                throw new ObjectAlreadyExistsException("At least one trigger already exists");
            }

            var jobIdsToAdd = triggersAndJobs.Select
            (
                x => x.Key.Key.GetDatabaseId(InstanceName)
            );
            
            var jobExists = await (
                from job in session.Query<Job>()
                where job.Id.In(jobIdsToAdd)
                select job
            ).AnyAsync(token).ConfigureAwait(false);

            if (jobExists)
            {
                TraceExit(Logger, nameof(ObjectAlreadyExistsException));
                throw new ObjectAlreadyExistsException("At least one job already exists");
            }
        }
        
        await using var bulkInsert = DocumentStore.ThrowIfNull().BulkInsert(token: token);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        var pausedTriggerGroups = await GetPausedTriggerGroupsAsync
        (
            session,
            token
        ).ConfigureAwait(false);
        var pausedJobGroups = await GetPausedJobGroupsAsync
        (
            session,
            token
        ).ConfigureAwait(false);

        foreach (var (job, triggers) in triggersAndJobs)
        {
            await bulkInsert
                .StoreAsync(new Job(job, InstanceName))
                .ConfigureAwait(false);

            foreach (var trigger in triggers.OfType<IOperableTrigger>())
            {
                var triggerToInsert = new Trigger(trigger, InstanceName);
                var isInPausedGroup = pausedTriggerGroups.Contains(triggerToInsert.Group)
                                      ||
                                      pausedJobGroups.Contains(trigger.JobKey.Group);

                if (isInPausedGroup)
                {
                    triggerToInsert.State = InternalTriggerState.Paused;

                    if (scheduler.BlockedJobs.Contains(trigger.JobKey.GetDatabaseId(InstanceName)))
                    {
                        triggerToInsert.State = InternalTriggerState.PausedAndBlocked;
                    }
                }
                else if (scheduler.BlockedJobs.Contains(trigger.JobKey.GetDatabaseId(InstanceName)))
                {
                    triggerToInsert.State = InternalTriggerState.Blocked;
                }

                await bulkInsert
                    .StoreAsync(triggerToInsert, triggerToInsert.Id)
                    .ConfigureAwait(false);
            }
        }
        
        TraceExit(Logger);
    }

    internal async Task<bool> RemoveJobAsync(JobKey jobKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var jobExists = await session.Advanced
            .ExistsAsync(jobKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (jobExists == false)
        {
            TraceExit(Logger, false);
            return false;
        }

        session.Delete(jobKey.GetDatabaseId(InstanceName));

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);

        TraceExit(Logger, true);
        return true;
    }

    internal async Task<bool> RemoveJobsAsync(IEnumerable<JobKey> jobKeys, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        foreach (var jobKey in jobKeys)
        {
            session.Delete(jobKey.GetDatabaseId(InstanceName));
        }

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, true);

        return true;
    }

    internal async Task<IJobDetail?> RetrieveJobAsync(JobKey jobKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var job = await session
            .LoadAsync<Job>(jobKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);
        
        var result = job?.Item;

        TraceExit(Logger, result);
        
        return result;
    }

    internal async Task StoreTriggerAsync(
        IOperableTrigger newTrigger,
        bool replaceExisting,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggerExists = await session.Advanced
            .ExistsAsync(newTrigger.Key.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (triggerExists && replaceExisting == false)
        {
            TraceExit(Logger, nameof(ObjectAlreadyExistsException));
            throw new ObjectAlreadyExistsException(newTrigger);
        }

        var jobExists = await session.Advanced
            .ExistsAsync(newTrigger.JobKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (jobExists == false)
        {
            TraceExit(Logger, nameof(JobPersistenceException));
            throw new JobPersistenceException($"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
        }

        var trigger = await CreateConfiguredTriggerAsync(newTrigger, session, token);

        await session
            .StoreAsync(trigger, trigger.Id, token)
            .ConfigureAwait(false);
        
        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task<bool> RemoveTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggerExists = await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (triggerExists == false)
        {
            TraceExit(Logger, false);
            return false;
        }
        
        var trigger = await session
            .Include<Trigger>(x => x.JobId)
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        var job = await session
                .LoadAsync<Job>(trigger.JobId, token)
                .ConfigureAwait(false);

        var triggersForJob = await GetTriggersForJobKeysAsync
        (
            session,
            new[] { trigger.JobId },
            token
        ).ConfigureAwait(false);

        if (triggersForJob.Count == 1 && job.Durable == false)
        {
            session.Delete(job.Id);
            await Signaler.NotifySchedulerListenersJobDeleted(job.JobKey, token).ConfigureAwait(false);
        }
        
        session.Delete(trigger);

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);

        TraceExit(Logger, true);
        return true;
    }

    internal async Task<bool> RemoveTriggersAsync(
        IReadOnlyCollection<TriggerKey> triggerKeys,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggers = await session
            .Include<Trigger>(x => x.JobId)
            .LoadAsync<Trigger>(triggerKeys.Select(x => x.GetDatabaseId(InstanceName)), token)
            .ConfigureAwait(false);

        var jobKeys = triggers
            .Where(x => x.Value != null)
            .Select(x => x.Value!.JobId)
            .ToList();

        var jobs = await session
            .LoadAsync<Job>(jobKeys, token)
            .ConfigureAwait(false);

        var triggersForJobs = await GetTriggersForJobKeysAsync
        (
            session,
            jobKeys,
            token
        ).ConfigureAwait(false);

        var triggersToKeep = triggersForJobs
            .Where(x => triggerKeys.Any(key => key.Equals(x.TriggerKey)) == false)
            .ToList();

        var existingTriggers = triggers
            .Where(x => x.Value != null)
            .Select(x => x.Value)
            .ToList();

        var result = existingTriggers.Count == triggers.Count;

        foreach (var trigger in existingTriggers)
        {
            var triggersForJob = triggersToKeep.Count(x => x.JobId == trigger.JobId);
            if (triggersForJob == 0)
            {
                if (jobs.TryGetValue(trigger.JobId, out var job))
                {
                    if (job.Durable == false)
                    {
                        session.Delete(trigger.JobId);

                        jobs.Remove(trigger.JobId);
                        await Signaler
                            .NotifySchedulerListenersJobDeleted(job.JobKey, token)
                            .ConfigureAwait(false);
                    }
                }
            }
            
            session.Delete(trigger.Id);
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task<bool> ReplaceTriggerAsync(
        TriggerKey triggerKey,
        IOperableTrigger newTrigger,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggerExists = await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (triggerExists == false)
        {
            TraceExit(Logger, false);
            return false;
        }
        
        var jobExists = await session.Advanced
            .ExistsAsync(newTrigger.JobKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);
        
        if (jobExists == false)
        {
            TraceExit(Logger, nameof(JobPersistenceException));
            throw new JobPersistenceException($"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
        }
        
        var triggerToStore = await CreateConfiguredTriggerAsync
        (
            newTrigger,
            session, token).ConfigureAwait(false);

        await session
            .StoreAsync(triggerToStore, triggerToStore.Id, token)
            .ConfigureAwait(false);

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);

        TraceExit(Logger, true);
        return true;
    }

    internal async Task<IOperableTrigger?> RetrieveTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        var result = trigger?.Item;
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task<bool> CalendarExistsAsync(string calName, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await session
            .Advanced
            .ExistsAsync(Entities.Calendar.GetId(InstanceName, calName), token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task<bool> CheckExistsAsync(JobKey jobKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await session.Advanced
            .ExistsAsync(jobKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, result);
        return result;
    }

    internal async Task<bool> CheckExistsAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, result);
        return result;
    }

    internal async Task ClearAllSchedulingDataAsync(CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
            where trigger.Scheduler == InstanceName
            select trigger.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var jobs = await (
            from job in session.Query<Job>()
            where job.Scheduler == InstanceName
            select job.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var pausedTriggerGroups = await (
            from pausedTriggerGroup in session.Query<PausedTriggerGroup>()
            where pausedTriggerGroup.Scheduler == InstanceName
            select pausedTriggerGroup.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var pausedJobGroups = await (
            from pausedJobGroup in session.Query<PausedJobGroup>()
            where pausedJobGroup.Scheduler == InstanceName
            select pausedJobGroup.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var calendars = await (
            from calendar in session.Query<Entities.Calendar>()
            where calendar.Scheduler == InstanceName
            select calendar.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        
        scheduler.BlockedJobs.Clear();
        
        triggers.ForEach(x => session.Delete(x));
        jobs.ForEach(x => session.Delete(x));
        pausedTriggerGroups.ForEach(x => session.Delete(x));
        pausedJobGroups.ForEach(x => session.Delete(x));
        calendars.ForEach(x => session.Delete(x));

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);

        TraceExit(Logger);
    }

    internal async Task StoreCalendarAsync(
        string name,
        ICalendar calendar,
        bool replaceExisting,
        bool updateTriggers,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var calendarToStore = calendar.Clone();
        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        scheduler.ThrowIfNull();

        var calendarExists = await session
            .Advanced
            .ExistsAsync(Entities.Calendar.GetId(InstanceName, name), token)
            .ConfigureAwait(false);

        if (calendarExists)
        {
            throw new ObjectAlreadyExistsException($"Calendar with name '{name}' already exists");
        }

        await session.StoreAsync
        (
            new Entities.Calendar(calendarToStore, name, InstanceName),
            token
        );

        if (updateTriggers)
        {
            var triggersToUpdate = await (
                from trigger in session.Query<Trigger>()
                where trigger.CalendarName == name
                select trigger
            ).ToListAsync(token).ConfigureAwait(false);

            Logger.LogTrace("Found {Count} triggers to update", triggersToUpdate.Count);

            foreach (var trigger in triggersToUpdate)
            {
                var operableTrigger = trigger.Item.ThrowIfNull();
                operableTrigger.UpdateWithNewCalendar(calendarToStore, MisfireThreshold);
                
                trigger.Item = operableTrigger;
            }
        }

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task<bool> RemoveCalendarAsync(string calendarName, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var exists = await session
            .Advanced
            .ExistsAsync(Entities.Calendar.GetId(InstanceName, calendarName), token)
            .ConfigureAwait(false);

        if (exists == false)
        {
            TraceExit(Logger, false);
            return false;
        }
        
        session.Delete(Entities.Calendar.GetId(InstanceName, calendarName));

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, true);

        return true;
    }

    internal async Task<ICalendar?> RetrieveCalendarAsync(string calendarName, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var calendar = await session
            .LoadAsync<Entities.Calendar>(Entities.Calendar.GetId(InstanceName, calendarName), token)
            .ConfigureAwait(false);

        TraceExit(Logger, calendar?.Item);

        return calendar?.Item;
    }

    internal async Task<int> GetNumberOfJobsAsync(CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var result = await (
            from job in session.Query<Job>()
            select job
        ).CountAsync(token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<int> GetNumberOfTriggersAsync(CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var result = await (
            from trigger in session.Query<Trigger>()
            select trigger
        ).CountAsync(token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<int> GetNumberOfCalendarsAsync(CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var result = await (
            from calendar in session.Query<Entities.Calendar>()
            where calendar.Scheduler == InstanceName
            select calendar
        ).CountAsync(token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<IReadOnlyCollection<JobKey>> GetJobKeysAsync(
        GroupMatcher<JobKey> matcher,
            CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetNonWaitingSession();
        
        var query = session.Query<Job>().ProjectInto<JobKey>();

        await using var stream = await session
            .Advanced
            .StreamAsync(query, token)
            .ConfigureAwait(false);

        var result = new HashSet<JobKey>();

        while (await stream.MoveNextAsync().ConfigureAwait(false))
        {
            if (matcher.IsMatch(stream.Current.Document)) result.Add(stream.Current.Document);
        }

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeysAsync(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetNonWaitingSession();
        
        var query = session.Query<Trigger>().ProjectInto<TriggerKey>();

        await using var stream = await session
            .Advanced
            .StreamAsync(query, token)
            .ConfigureAwait(false);

        var result = new HashSet<TriggerKey>();

        while (await stream.MoveNextAsync().ConfigureAwait(false))
        {
            if (matcher.IsMatch(stream.Current.Document)) result.Add(stream.Current.Document);
        }

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<IReadOnlyCollection<string>> GetJobGroupNamesAsync(CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await (
            from job in session.Query<Job>()
            group job by job.Group
            into jobGroups
            select new { Group = jobGroups.Key }
        ).ToListAsync(token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result.Select(x => x.Group).ToList();
    }

    internal async Task<IReadOnlyCollection<string>> GetTriggerGroupNamesAsync(CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await GetTriggerGroupNamesAsync(session, token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<IReadOnlyCollection<string>> GetCalendarNamesAsync(CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await (
            from calendar in session.Query<Entities.Calendar>()
            where calendar.Scheduler == InstanceName
            select calendar.Name
        ).ToListAsync(token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result;
    }
    
    internal async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(
        JobKey jobKey,
        CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var jobId = jobKey.GetDatabaseId(InstanceName);

        var triggers = await (
            from trigger in session.Query<Trigger>()
            where trigger.JobId == jobId
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var result = triggers.Select(x => x.Item.ThrowIfNull()).ToList();

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (trigger == null) return TriggerState.None;

        var result = trigger.State switch
        {
            InternalTriggerState.Complete => TriggerState.Complete,
            InternalTriggerState.Blocked => TriggerState.Blocked,
            InternalTriggerState.PausedAndBlocked => TriggerState.Paused,
            InternalTriggerState.Paused => TriggerState.Paused,
            InternalTriggerState.Error => TriggerState.Error,
            _ => TriggerState.Normal
        };

        TraceExit(Logger, result);

        return result;
    }

    internal async Task ResetTriggerFromErrorStateAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (trigger is not { State: InternalTriggerState.Error })
        {
            TraceExit(Logger);
            return;
        }

        var isTriggerGroupPaused = await IsTriggerGroupPausedAsync
        (
            session,
            trigger.Group,
            token
        ).ConfigureAwait(false);

        var scheduler = await session.LoadAsync<Scheduler>(InstanceName, token).ConfigureAwait(false);

        var isJobGroupPaused = await IsJobGroupPausedAsync
        (
            session,
            trigger.JobGroup,
            token
        ).ConfigureAwait(false);

        if (isTriggerGroupPaused || isJobGroupPaused)
        {
            trigger.State = InternalTriggerState.Paused;

            if (scheduler.BlockedJobs.Contains(trigger.JobId))
            {
                trigger.State = InternalTriggerState.PausedAndBlocked;
            }
        }
        else if (scheduler.BlockedJobs.Contains(trigger.JobId))
        {
            trigger.State = InternalTriggerState.Blocked;
        }
        else
        {
            trigger.State = InternalTriggerState.Waiting;
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task PauseTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (trigger == null || trigger.State == InternalTriggerState.Complete) return;

        trigger.State = trigger.State == InternalTriggerState.Blocked
            ? InternalTriggerState.PausedAndBlocked
            : InternalTriggerState.Paused;

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task<IReadOnlyCollection<string>> PauseTriggersAsync(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetNonWaitingSession();
        using var updateSession = GetSession();
        
        var query = session.Query<Trigger>();

        await using var stream = await session
            .Advanced
            .StreamAsync(query, token)
            .ConfigureAwait(false);

        var result = new HashSet<string>();

        while (await stream.MoveNextAsync().ConfigureAwait(false))
        {
            if (matcher.IsMatch(stream.Current.Document.TriggerKey) == false) continue;
            
            result.Add(stream.Current.Document.Group);

            if (stream.Current.Document.State == InternalTriggerState.Complete) continue;

            stream.Current.Document.State = stream.Current.Document.State == InternalTriggerState.Blocked
                ? InternalTriggerState.PausedAndBlocked 
                : InternalTriggerState.Paused;

            await updateSession.StoreAsync
            (
                stream.Current.Document,
                stream.Current.ChangeVector,
                stream.Current.Id,
                token
            ).ConfigureAwait(false);
        }

        if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
        {
            await EnsurePausedTriggerGroupAsync(updateSession, matcher.CompareToValue, token).ConfigureAwait(false);
        }

        await updateSession.SaveChangesAsync(token).ConfigureAwait(false);

        TraceExit(Logger, result);

        return result;
    }

    internal async Task PauseJobAsync(JobKey jobKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggers = await GetTriggersForJobKeysAsync
        (
            session,
            new[] { jobKey.GetDatabaseId(InstanceName) },
            token
        ).ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            trigger.State = trigger.State == InternalTriggerState.Blocked
                ? InternalTriggerState.PausedAndBlocked
                : InternalTriggerState.Paused;
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task<IReadOnlyCollection<string>> PauseJobsAsync(
        GroupMatcher<JobKey> matcher,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var jobKeys = await (
            from job in session.Query<Job>()
            select new { job.Name, job.Group }
        ).ToListAsync(token).ConfigureAwait(false);

        var matchedJobKeys = new HashSet<string>();
        var result = new HashSet<string>();

        jobKeys.ForEach(x =>
        {
            var jobKey = new JobKey(x.Name, x.Group);
            if (matcher.IsMatch(jobKey) == false) return;
            
            matchedJobKeys.Add(jobKey.GetDatabaseId(InstanceName));
            result.Add(x.Group);
        });
        var triggers = await GetTriggersForJobKeysAsync
        (
            session,
            matchedJobKeys.ToList(),
            token
        ).ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            trigger.State = trigger.State == InternalTriggerState.Blocked
                ? InternalTriggerState.PausedAndBlocked
                : InternalTriggerState.Paused;
        }

        if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
        {
            await EnsurePausedJobGroupAsync(session, matcher.CompareToValue, token).ConfigureAwait(false);
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task ResumeTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var trigger = await session
            .Include<Trigger>(x => x.CalendarId) // preload
            .Include<Trigger>(x => x.Scheduler) // preload
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (trigger == null) return;
        if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) return;

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        trigger.State = scheduler.BlockedJobs.Contains(trigger.JobId)
            ? InternalTriggerState.Blocked
            : InternalTriggerState.Waiting;

        await ApplyMisfireAsync(session, trigger, token).ConfigureAwait(false);

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task<IReadOnlyCollection<string>> ResumeTriggersAsync(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.Scheduler)
                .Include(x => x.CalendarId)
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = new HashSet<string>();

        // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
        foreach (var trigger in triggers)
        {
            if (matcher.IsMatch(trigger.TriggerKey) == false) continue;
            if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) continue;
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobId)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(session, trigger, token).ConfigureAwait(false);

            result.Add(trigger.TriggerKey.Group);
        }

        if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
        {
            session.Delete(PausedTriggerGroup.GetId(InstanceName, matcher.CompareToValue));
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    private async Task<IReadOnlyList<string>> GetPausedTriggerGroupsAsync(CancellationToken token)
    {
        using var session = GetSession();
        return await GetPausedTriggerGroupsAsync(session, token).ConfigureAwait(false);
    }

    internal async Task ResumeJobAsync(JobKey jobKey, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggers = await GetTriggersForJobKeysAsync
        (
            session,
            new[] { jobKey.GetDatabaseId(InstanceName) },
            token
        ).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) continue;
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobId)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(session, trigger, token).ConfigureAwait(false);
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task ResumeAllTriggersAsync(CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.CalendarId)
                .Include(x => x.Scheduler)
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) continue;
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobId)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(session, trigger, token).ConfigureAwait(false);
        }

        var groups = await GetPausedTriggerGroupsAsync(session, token).ConfigureAwait(false);

        foreach (var group in groups)
        {
            session.Delete(PausedTriggerGroup.GetId(InstanceName, group));
        }
        
        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    internal async Task<IReadOnlyCollection<string>> ResumeJobsAsync(
        GroupMatcher<JobKey> matcher,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var jobKeys = await (
            from job in session.Query<Job>()
                .Include(x => x.Scheduler)
            select new { job.Name, job.Group }
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var matchedJobKeys = new HashSet<string>();
        var result = new HashSet<string>();

        jobKeys.ForEach(x =>
        {
            var jobKey = new JobKey(x.Name, x.Group);
            if (matcher.IsMatch(jobKey) == false) return;
            
            matchedJobKeys.Add(jobKey.GetDatabaseId(InstanceName));
            result.Add(x.Group);
        });
        var triggers = await GetTriggersForJobKeysAsync
        (
            session,
            matchedJobKeys.ToList(),
            token
        ).ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) continue;
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobId)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(session, trigger, token).ConfigureAwait(false);
        }

        if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
        {
            session.Delete(PausedJobGroup.GetId(InstanceName, matcher.CompareToValue));
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersAsync(
        DateTimeOffset noLaterThan,
        int maxCount,
        TimeSpan timeWindow,
        CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var result = new List<IOperableTrigger>();
        var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

        var upperLimit = noLaterThan + timeWindow;

        var storedTriggers = await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.CalendarId)
                .Include(x => x.JobId)
            where trigger.State == InternalTriggerState.Waiting
                  &&
                  trigger.NextFireTimeUtc <= upperLimit 
            orderby trigger.NextFireTimeUtc,
                    trigger.Priority descending
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        // Copy the list to another list, we may need to modify it.
        var candidateTriggers = new PriorityQueue<Trigger, int>(storedTriggers.Select(x => (x, -x.Priority)));

        while (candidateTriggers.Count > 0 && result.Count < maxCount)
        {
            var trigger = candidateTriggers.Dequeue();
            if (trigger.NextFireTimeUtc == null) continue;
            
            var misfireApplied = await ApplyMisfireAsync(session, trigger, token).ConfigureAwait(false);
            if (misfireApplied)
            {
                if (trigger.NextFireTimeUtc != null)
                {
                    candidateTriggers.Enqueue(trigger, -trigger.Priority);
                    continue;
                }
            }
            
            if (trigger.NextFireTimeUtc > noLaterThan + timeWindow) break;

            var job = await session
                .LoadAsync<Job>(trigger.JobId, token)
                .ConfigureAwait(false);

            if (job.Item.ThrowIfNull().ConcurrentExecutionDisallowed)
            {
                var jobKey = new JobKey(job.Name, job.Group);
                
                if (acquiredJobKeysForNoConcurrentExec.Add(jobKey) == false) continue;
            }

            trigger.State = InternalTriggerState.Acquired;
            trigger.FireInstanceId = GetFiredTriggerRecordId();

            result.Add(trigger.Item.ThrowIfNull());
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    private async Task ReleaseAcquiredTriggerAsync(IMutableTrigger trigger, CancellationToken token)
    {
        using var session = GetSession();

        var storedTrigger = await session
            .LoadAsync<Trigger>(trigger.Key.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);
        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        
        if (storedTrigger is null || storedTrigger.State != InternalTriggerState.Acquired) return;

        storedTrigger.State = scheduler.BlockedJobs.Contains(storedTrigger.JobId)
            ? InternalTriggerState.Blocked
            : InternalTriggerState.Waiting;

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }

    private async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFiredAsync(
        IEnumerable<IOperableTrigger> triggers,
        CancellationToken token)
    {
        using var session = GetSession();

        var result = new List<TriggerFiredResult>();

        var triggerKeys = triggers
            .Select(x => x.Key.GetDatabaseId(InstanceName))
            .ToList();

        var storedTriggers = await session
            .Include<Trigger>(x => x.JobId)
            .Include<Trigger>(x => x.CalendarId)
            .LoadAsync<Trigger>(triggerKeys, token)
            .ConfigureAwait(false);

        foreach (var (_, storedTrigger) in storedTriggers)
        {
            if (storedTrigger?.State != InternalTriggerState.Acquired) continue;

            var calendar = await session
                .LoadAsync<Entities.Calendar>(storedTrigger.CalendarId, token)
                .ConfigureAwait(false);

            var operableTrigger = storedTrigger.Item.ThrowIfNull();
            var previousFireTime = operableTrigger.GetPreviousFireTimeUtc();
            operableTrigger.Triggered(calendar?.Item);

            var storedJob = await session
                .LoadAsync<Job>(storedTrigger.JobId, token)
                .ConfigureAwait(false);

            var jobDetail = storedJob.Item.ThrowIfNull();

            var bundle = new TriggerFiredBundle(
                jobDetail,
                operableTrigger,
                calendar?.Item,
                false,
                SystemTime.UtcNow(),
                operableTrigger.GetPreviousFireTimeUtc(),
                previousFireTime,
                operableTrigger.GetNextFireTimeUtc()
            );

            if (jobDetail.ConcurrentExecutionDisallowed)
            {
                var triggersToBlock = await (
                    from trigger in session.Query<Trigger>()
                        .Include(x => x.Scheduler)
                    where trigger.JobId == storedTrigger.JobId
                          &&
                          trigger.Id != storedTrigger.Id
                    select trigger
                ).ToListAsync(token).ConfigureAwait(false);

                foreach (var trigger in triggersToBlock)
                {
                    trigger.State = trigger.State switch
                    {
                        InternalTriggerState.Waiting => InternalTriggerState.Blocked,
                        InternalTriggerState.Paused => InternalTriggerState.PausedAndBlocked,
                        _ => trigger.State
                    };

                    var thatScheduler = await session
                        .LoadAsync<Scheduler>(InstanceName, token)
                        .ConfigureAwait(false);

                    thatScheduler.BlockedJobs.Add(jobDetail.Key.GetDatabaseId(InstanceName));
                }
            }

            await session.SaveChangesAsync(token).ConfigureAwait(false);

            result.Add(new TriggerFiredResult(bundle));
        }

        return result;
    }

    private async Task TriggeredJobCompleteAsync(
        IMutableTrigger trigger,
        IJobDetail jobDetail,
        SchedulerInstruction triggerInstCode,
        CancellationToken token)
    {
        using var session = GetSession();

        var entry = await session
            .Include<Trigger>(x => x.Scheduler)
            .Include<Trigger>(x => x.JobId)
            .LoadAsync<Trigger>(trigger.Key.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        var job = await session
            .LoadAsync<Job>(trigger.JobKey.GetDatabaseId(InstanceName), token)
            .ConfigureAwait(false);

        if (job != null)
        {
            if (jobDetail.PersistJobDataAfterExecution) job.Item = jobDetail;

            if (jobDetail.ConcurrentExecutionDisallowed)
            {
                scheduler.BlockedJobs.Remove(job.Id);

                var triggersForJob = await (
                    from item in session.Query<Trigger>()
                    where item.JobId == job.Id
                    select item
                ).ToListAsync(token).ConfigureAwait(false);

                foreach (var item in triggersForJob)
                {
                    item.State = item.State switch
                    {
                        InternalTriggerState.Blocked => InternalTriggerState.Waiting,
                        InternalTriggerState.PausedAndBlocked => InternalTriggerState.Paused,
                        _ => item.State
                    };
                }

                Signaler.SignalSchedulingChange(null, token);
            }
        }
        else
        {
            // even if it was deleted, there may be cleanup to do
            scheduler.BlockedJobs.Remove(jobDetail.Key.GetDatabaseId(InstanceName));
        }

        switch (triggerInstCode)
        {
            case SchedulerInstruction.ReExecuteJob:
                break;
            case SchedulerInstruction.NoInstruction:
                break;
            case SchedulerInstruction.DeleteTrigger:
            {
                // Deleting triggers
                var nextFireTime = trigger.GetNextFireTimeUtc();
                if (nextFireTime.HasValue == false)
                {
                    nextFireTime = entry.NextFireTimeUtc;
                    if (nextFireTime.HasValue == false)
                    {
                        session.Delete(trigger.Key.GetDatabaseId(InstanceName));
                    }
                }
                else
                {
                    session.Delete(trigger.Key.GetDatabaseId(InstanceName));
                    Signaler.SignalSchedulingChange(null, token);
                }

                break;
            }
            case SchedulerInstruction.SetTriggerComplete:
                entry.State = InternalTriggerState.Complete;
                Signaler.SignalSchedulingChange(null, token);
                break;

            case SchedulerInstruction.SetTriggerError:
                entry.State = InternalTriggerState.Error;
                Signaler.SignalSchedulingChange(null, token);
                break;
            
            case SchedulerInstruction.SetAllJobTriggersError:
                await SetAllTriggersOfJobToStateAsync
                (
                    session,
                    trigger.JobKey,
                    InternalTriggerState.Error,
                    token
                ).ConfigureAwait(false);
                Signaler.SignalSchedulingChange(null, token);
                break;
            
            case SchedulerInstruction.SetAllJobTriggersComplete:
                await SetAllTriggersOfJobToStateAsync
                (
                    session,
                    trigger.JobKey,
                    InternalTriggerState.Complete,
                    token
                ).ConfigureAwait(false);
                Signaler.SignalSchedulingChange(null, token);
                break;
            default:
                throw new UnreachableException("Unexpected case");
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }
}
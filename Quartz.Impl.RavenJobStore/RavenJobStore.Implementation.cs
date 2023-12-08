using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Quartz.Impl.Matchers;
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
        
        Logger.LogDebug("Scheduler started at {0}", SystemTime.UtcNow());
        
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
            .StoreAsync(triggerToStore, triggerToStore.Key, cancellationToken)
            .ConfigureAwait(false);

        await session
            .StoreAsync(jobToStore, jobToStore.Key, cancellationToken)
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

        if (await session.Advanced.ExistsAsync(newJob.Key.GetDatabaseId(), token).ConfigureAwait(false))
        {
            if (replaceExisting == false)
            {
                TraceExit(Logger, nameof(ObjectAlreadyExistsException));
                throw new ObjectAlreadyExistsException(newJob);
            }
        }

        var job = new Job(newJob, InstanceName);

        await session.StoreAsync(job, job.Key, token).ConfigureAwait(false);
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
            .SelectMany(x => x.Value.Select(t => t.Key.GetDatabaseId()))
            .ToList();

        if (replace == false)
        {
            var triggerExists = await (
                from trigger in session.Query<Trigger>()
                where trigger.Key.In(triggerIdsToAdd)
                select trigger
            ).AnyAsync(token).ConfigureAwait(false);

            if (triggerExists)
            {
                TraceExit(Logger, nameof(ObjectAlreadyExistsException));
                throw new ObjectAlreadyExistsException("At least one trigger already exists");
            }
            
            var jobExists = await (
                from job in session.Query<Job>()
                where job.Key.In(triggersAndJobs.Select(x => x.Key.Key.GetDatabaseId()))
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
                .StoreAsync(new Job(job, InstanceName), job.Key.GetDatabaseId())
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

                    if (scheduler.BlockedJobs.Contains(trigger.JobKey.GetDatabaseId()))
                    {
                        triggerToInsert.State = InternalTriggerState.PausedAndBlocked;
                    }
                }
                else if (scheduler.BlockedJobs.Contains(trigger.JobKey.GetDatabaseId()))
                {
                    triggerToInsert.State = InternalTriggerState.Blocked;
                }

                await bulkInsert
                    .StoreAsync(triggerToInsert, triggerToInsert.Key)
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
            .ExistsAsync(jobKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (jobExists == false)
        {
            TraceExit(Logger, false);
            return false;
        }

        session.Delete(jobKey.GetDatabaseId());

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
            session.Delete(jobKey.GetDatabaseId());
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
            .LoadAsync<Job>(jobKey.GetDatabaseId(), token)
            .ConfigureAwait(false);
        
        var result = job?.Deserialize();

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
            .ExistsAsync(newTrigger.Key.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (triggerExists && replaceExisting == false)
        {
            TraceExit(Logger, nameof(ObjectAlreadyExistsException));
            throw new ObjectAlreadyExistsException(newTrigger);
        }

        var jobExists = await session.Advanced
            .ExistsAsync(newTrigger.JobKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (jobExists == false)
        {
            TraceExit(Logger, nameof(JobPersistenceException));
            throw new JobPersistenceException($"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
        }

        var trigger = await CreateConfiguredTriggerAsync(newTrigger, session, token);

        await session
            .StoreAsync(trigger, trigger.Key, token)
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
            .ExistsAsync(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (triggerExists == false)
        {
            TraceExit(Logger, false);
            return false;
        }
        
        var trigger = await session
            .Include<Trigger>(x => x.JobKey)
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        var job = await session
                .LoadAsync<Job>(trigger.JobKey, token)
                .ConfigureAwait(false);

        var triggersForJob = await GetTriggersForJobKeysAsync
        (
            session,
            new[] { trigger.JobKey },
            token
        ).ConfigureAwait(false);

        if (triggersForJob.Count == 1 && job.Durable == false)
        {
            session.Delete(job.Key);
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
            .Include<Trigger>(x => x.JobKey)
            .LoadAsync<Trigger>(triggerKeys.Select(x => x.GetDatabaseId()), token)
            .ConfigureAwait(false);

        var jobKeys = triggers
            .Where(x => x.Value != null)
            .Select(x => x.Value!.JobKey)
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
            var triggersForJob = triggersToKeep.Count(x => x.JobKey == trigger.JobKey);
            if (triggersForJob == 0)
            {
                if (jobs.TryGetValue(trigger.JobKey, out var job))
                {
                    if (job.Durable == false)
                    {
                        session.Delete(trigger.JobKey);

                        jobs.Remove(trigger.JobKey);
                        await Signaler
                            .NotifySchedulerListenersJobDeleted(job.JobKey, token)
                            .ConfigureAwait(false);
                    }
                }
            }
            
            session.Delete(trigger.Key);
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
            .ExistsAsync(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (triggerExists == false)
        {
            TraceExit(Logger, false);
            return false;
        }
        
        var jobExists = await session.Advanced
            .ExistsAsync(newTrigger.JobKey.GetDatabaseId(), token)
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
            .StoreAsync(triggerToStore, triggerToStore.Key, token)
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
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        var result = trigger?.Deserialize();
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task<bool> CalendarExistsAsync(string calName, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = scheduler?.Calendars is not null && scheduler.Calendars.ContainsKey(calName);
        
        TraceExit(Logger, result);
        return result;
    }

    internal async Task<bool> CheckExistsAsync(JobKey jobKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await session.Advanced
            .ExistsAsync(jobKey.GetDatabaseId(), token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, result);
        return result;
    }

    internal async Task<bool> CheckExistsAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var result = await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, result);
        return result;
    }

    internal async Task ClearAllSchedulingDataAsync(CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var triggerKeys = await (
            from trigger in session.Query<Trigger>()
            where trigger.Scheduler == InstanceName
            select trigger.Key
        ).ToListAsync(token).ConfigureAwait(false);

        var jobKeys = await (
            from job in session.Query<Job>()
            where job.Scheduler == InstanceName
            select job.Key
        ).ToListAsync(token).ConfigureAwait(false);

        var pausedTriggers = await (
            from pausedTrigger in session.Query<PausedTriggerGroup>()
            where pausedTrigger.Scheduler == InstanceName
            select pausedTrigger.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        
        scheduler.BlockedJobs.Clear();
        scheduler.Calendars.Clear();
        
        triggerKeys.ForEach(x => session.Delete(x));
        jobKeys.ForEach(x => session.Delete(x));
        pausedTriggers.ForEach(x => session.Delete(x));

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

        if (scheduler.Calendars.ContainsKey(name) && replaceExisting == false)
        {
            throw new ObjectAlreadyExistsException($"Calendar with name '{name}' already exists");
        }

        scheduler.Calendars[name] = calendarToStore;

        if (updateTriggers)
        {
            var triggersToUpdate = await (
                from trigger in session.Query<Trigger>()
                where trigger.CalendarName == name
                select trigger
            ).ToListAsync(token).ConfigureAwait(false);

            Logger.LogTrace("Found {0} triggers to update", triggersToUpdate.Count);

            foreach (var trigger in triggersToUpdate)
            {
                var operableTrigger = trigger.Deserialize();
                operableTrigger.UpdateWithNewCalendar(calendarToStore, MisfireThreshold);
                trigger.UpdateFireTimes(operableTrigger);
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

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = scheduler.ThrowIfNull().Calendars.Remove(calendarName);

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task<ICalendar?> RetrieveCalendarAsync(string calendarName, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = GetSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        if (scheduler.ThrowIfNull().Calendars.TryGetValue(calendarName, out var calendar))
        {
            TraceExit(Logger, calendar);
            return calendar;
        }

        TraceExit(Logger, (ICalendar?)null);

        return null;
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

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = scheduler.ThrowIfNull().Calendars.Count;

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

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = scheduler.ThrowIfNull().Calendars.Keys;

        TraceExit(Logger, result);

        return result;
    }
    
    internal async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(
        JobKey jobKey,
        CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
            where trigger.JobKey == jobKey.GetDatabaseId()
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var result = triggers.Select(x => x.Deserialize()).ToList();

        TraceExit(Logger, result);

        return result;
    }

    internal async Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey, CancellationToken token)
    {
        TraceEnter(Logger);

        using var session = GetSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
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
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
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

            if (scheduler.BlockedJobs.Contains(trigger.JobKey))
            {
                trigger.State = InternalTriggerState.PausedAndBlocked;
            }
        }
        else if (scheduler.BlockedJobs.Contains(trigger.JobKey))
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
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
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
            new[] { jobKey.GetDatabaseId() },
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
            where job.Scheduler == InstanceName
            select job.JobKey
        ).ToListAsync(token).ConfigureAwait(false);

        var matchedJobKeys = new HashSet<string>();
        var result = new HashSet<string>();

        jobKeys.ForEach(x =>
        {
            if (matcher.IsMatch(x) == false) return;
            
            matchedJobKeys.Add(x.GetDatabaseId());
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
            .Include<Trigger>(x => x.Scheduler) // preload
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (trigger == null) return;
        if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) return;

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        trigger.State = scheduler.BlockedJobs.Contains(trigger.JobKey)
            ? InternalTriggerState.Blocked
            : InternalTriggerState.Waiting;

        await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);

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
            where trigger.Scheduler == InstanceName
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
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobKey)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);

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
            new[] { jobKey.GetDatabaseId() },
            token
        ).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) continue;
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobKey)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);
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
                .Include(x => x.Scheduler)
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            if (trigger.State is not InternalTriggerState.Paused and not InternalTriggerState.PausedAndBlocked) continue;
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobKey)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);
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
            where job.Scheduler == InstanceName
            select job.JobKey
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var matchedJobKeys = new HashSet<string>();
        var result = new HashSet<string>();

        jobKeys.ForEach(x =>
        {
            if (matcher.IsMatch(x) == false) return;
            
            matchedJobKeys.Add(x.GetDatabaseId());
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
        
            trigger.State = scheduler.BlockedJobs.Contains(trigger.JobKey)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);
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

        var upperLimit = (noLaterThan + timeWindow).Ticks;

        var storedTriggers = await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.Scheduler)
                .Include(x => x.JobKey)
            where trigger.Scheduler == InstanceName
                  &&
                  trigger.State == InternalTriggerState.Waiting
                  &&
                  trigger.NextFireTimeTicks <= upperLimit 
            orderby trigger.NextFireTimeTicks,
                    trigger.Priority descending
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        // Copy the list to another list, we may need to modify it.
        var candidateTriggers = new PriorityQueue<Trigger, int>(storedTriggers.Select(x => (x, -x.Priority)));

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        while (candidateTriggers.Count > 0 && result.Count < maxCount)
        {
            var trigger = candidateTriggers.Dequeue();
            if (trigger.NextFireTimeUtc == null) continue;
            
            var misfireApplied = await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);
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
                .LoadAsync<Job>(trigger.JobKey, token)
                .ConfigureAwait(false);

            if (job.ConcurrentExecutionDisallowed)
            {
                var jobKey = new JobKey(job.Name, job.Group);
                
                if (acquiredJobKeysForNoConcurrentExec.Add(jobKey) == false) continue;
            }

            trigger.State = InternalTriggerState.Acquired;
            trigger.FireInstanceId = GetFiredTriggerRecordId();

            result.Add(trigger.Deserialize());
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    private async Task ReleaseAcquiredTriggerAsync(IMutableTrigger trigger, CancellationToken token)
    {
        using var session = GetSession();

        var storedTrigger = await session
            .LoadAsync<Trigger>(trigger.Key.GetDatabaseId(), token)
            .ConfigureAwait(false);
        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        
        if (storedTrigger is null || storedTrigger.State != InternalTriggerState.Acquired) return;

        storedTrigger.State = scheduler.BlockedJobs.Contains(storedTrigger.JobKey)
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
            .Select(x => x.Key.GetDatabaseId())
            .ToList();

        var storedTriggers = await session
            .Include<Trigger>(x => x.Scheduler)
            .Include<Trigger>(x => x.JobKey)
            .LoadAsync<Trigger>(triggerKeys, token)
            .ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        foreach (var (_, storedTrigger) in storedTriggers)
        {
            if (storedTrigger?.State != InternalTriggerState.Acquired) continue;

            var calendar = scheduler.Calendars.GetValueOrDefault(storedTrigger.CalendarName ?? string.Empty);

            if (calendar == null) continue;
            
            var previousFireTime = storedTrigger.PreviousFireTimeUtc;

            var operableTrigger = storedTrigger.Deserialize();
            operableTrigger.Triggered(calendar);

            var storedJob = await session
                .LoadAsync<Job>(storedTrigger.JobKey, token)
                .ConfigureAwait(false);

            var jobDetail = storedJob.Deserialize();

            var bundle = new TriggerFiredBundle(
                jobDetail,
                operableTrigger,
                calendar,
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
                    where trigger.JobKey == storedTrigger.JobKey
                          &&
                          trigger.Key != storedTrigger.Key
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

                    thatScheduler.BlockedJobs.Add(jobDetail.Key.GetDatabaseId());
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
            .Include<Trigger>(x => x.JobKey)
            .LoadAsync<Trigger>(trigger.Key.GetDatabaseId(), token)
            .ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        var job = await session
            .LoadAsync<Job>(trigger.JobKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (job != null)
        {
            if (jobDetail.PersistJobDataAfterExecution) job.JobDataMap = jobDetail.JobDataMap;

            if (job.ConcurrentExecutionDisallowed)
            {
                scheduler.BlockedJobs.Remove(job.Key);

                var triggersForJob = await (
                    from item in session.Query<Trigger>()
                    where item.JobKey == trigger.JobKey.GetDatabaseId()
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
            scheduler.BlockedJobs.Remove(jobDetail.Key.GetDatabaseId());
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
                        session.Delete(trigger.Key.GetDatabaseId());
                    }
                }
                else
                {
                    session.Delete(trigger.Key.GetDatabaseId());
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
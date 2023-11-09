using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
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

        store.OnBeforeQuery += (_, beforeQueryExecutedArgs) =>
        {
            beforeQueryExecutedArgs.QueryCustomization.WaitForNonStaleResults();
        };
        store.Initialize();

        return store;
    }

    internal async Task SchedulerStartedAsync(CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();
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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();
        var jobToStore = new Job(newJob, InstanceName);

        var triggerToStore = await CreateConfiguredTriggerAsync
        (
            newTrigger,
            cancellationToken,
            session
        ).ConfigureAwait(false);
        
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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = scheduler.ThrowIfNull().PausedJobGroups.Contains(groupName);

        TraceExit(Logger, result);
        
        return result;
    }

    internal async Task<bool> IsTriggerGroupPausedAsync(string groupName, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();
        var result =  await IsTriggerGroupPausedAsync(session, groupName, token).ConfigureAwait(false);
        
        TraceExit(Logger, result);

        return result;
    }

    internal async Task StoreJobAsync(IJobDetail newJob, bool replaceExisting, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        if (replace == false)
        {
            var triggerExists = await (
                from trigger in session.Query<Trigger>()
                where trigger.Key.In(triggersAndJobs.SelectMany(x => x.Value.Select(t => t.Key.GetDatabaseId())))
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
        
        await using var bulkInsert = DocumentStore.BulkInsert(token: token);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        var pausedTriggerGroups = await GetPausedTriggerGroupsAsync
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
                var isInPausedGroup = pausedTriggerGroups.Contains(triggerToInsert.Group);

                if (isInPausedGroup || scheduler.PausedJobGroups.Contains(trigger.JobKey.Group))
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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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

    internal async Task<bool> RemoveJobsAsync(IReadOnlyCollection<JobKey> jobKeys, CancellationToken token)
    {
        TraceEnter(Logger);
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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

        var trigger = await CreateConfiguredTriggerAsync(newTrigger, token, session);

        await session
            .StoreAsync(trigger, trigger.Key, token)
            .ConfigureAwait(false);
        
        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
        
        TraceExit(Logger);
    }

    private async Task<bool> RemoveTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var triggerExists = await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (triggerExists == false) return false;
        
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

        return true;
    }

    private async Task<bool> RemoveTriggersAsync(
        IReadOnlyCollection<TriggerKey> triggerKeys,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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

        var existingTriggers = triggers
            .Where(x => x.Value != null)
            .Select(x => x.Value)
            .ToList();

        var result = existingTriggers.Count == triggers.Count;

        foreach (var trigger in existingTriggers)
        {
            var triggersForJob = triggersForJobs.Count(x => x.JobKey == trigger.JobKey);
            if (triggersForJob == 1)
            {
                if (jobs.TryGetValue(trigger.JobKey, out var job))
                {
                    if (job.Durable == false)
                    {
                        session.Delete(trigger.JobKey);
                        await Signaler
                            .NotifySchedulerListenersJobDeleted(job.JobKey, token)
                            .ConfigureAwait(false);
                    }
                }
            }
            
            session.Delete(trigger.Key);
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);

        return result;
    }

    private async Task<bool> ReplaceTriggerAsync(
        TriggerKey triggerKey,
        IOperableTrigger newTrigger,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var exists = await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (exists == false) return false;
        
        session.Delete(triggerKey.GetDatabaseId());

        var triggerToStore = await CreateConfiguredTriggerAsync
        (
            newTrigger,
            token,
            session
        ).ConfigureAwait(false);

        await session
            .StoreAsync(triggerToStore, triggerToStore.Key, token)
            .ConfigureAwait(false);

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);

        return true;
    }

    private async Task<IOperableTrigger?> RetrieveTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        return trigger?.Deserialize();
    }

    private async Task<bool> CalendarExistsAsync(string calName, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        return scheduler?.Calendars is not null && scheduler.Calendars.ContainsKey(calName);
    }

    private async Task<bool> CheckExistsAsync(JobKey jobKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await session.Advanced
            .ExistsAsync(jobKey.GetDatabaseId(), token)
            .ConfigureAwait(false);
    }

    private async Task<bool> CheckExistsAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await session.Advanced
            .ExistsAsync(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);
    }

    private async Task ClearAllSchedulingDataAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var triggerKeys = await (
            from trigger in session.Query<Trigger>()
            select trigger.Key
        ).ToListAsync(token).ConfigureAwait(false);

        var jobKeys = await (
            from job in session.Query<Job>()
            select job.Key
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);
        
        scheduler.BlockedJobs.Clear();
        scheduler.PausedJobGroups.Clear();
        scheduler.Calendars.Clear();
        
        triggerKeys.ForEach(x => session.Delete(x));
        jobKeys.ForEach(x => session.Delete(x));

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);
    }

    private async Task StoreCalendarAsync(
        string name,
        ICalendar calendar,
        bool replaceExisting,
        bool updateTriggers,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
    }

    private async Task<bool> RemoveCalendarAsync(string calendarName, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = scheduler.ThrowIfNull().Calendars.Remove(calendarName);

        await session
            .SaveChangesAsync(token)
            .ConfigureAwait(false);

        return result;
    }

    private async Task<ICalendar?> RetrieveCalendarAsync(string calendarName, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        if (scheduler.ThrowIfNull().Calendars.TryGetValue(calendarName, out var calendar)) return calendar;

        return null;
    }

    private async Task<int> GetNumberOfJobsAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await (
            from job in session.Query<Job>()
            select job
        ).CountAsync(token).ConfigureAwait(false);
    }

    private async Task<int> GetNumberOfTriggersAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await (
            from trigger in session.Query<Trigger>()
            select trigger
        ).CountAsync(token).ConfigureAwait(false);
    }

    private async Task<int> GetNumberOfCalendarsAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        return scheduler.ThrowIfNull().Calendars.Count;
    }

    private async Task<IReadOnlyCollection<JobKey>> GetJobKeysAsync(
        GroupMatcher<JobKey> matcher,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var jobKeys = await (
            from job in session.Query<Job>()
            select job.JobKey
        ).ToListAsync(token).ConfigureAwait(false);

        var result = new HashSet<JobKey>();

        jobKeys.ForEach(x =>
        {
            if (matcher.IsMatch(x)) result.Add(x);
        });

        return result;
    }

    private async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeysAsync(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var triggerKeys = await (
            from trigger in session.Query<Trigger>()
            select trigger.TriggerKey
        ).ToListAsync(token).ConfigureAwait(false);

        var result = new HashSet<TriggerKey>();

        triggerKeys.ForEach(x =>
        {
            if (matcher.IsMatch(x)) result.Add(x);
        });

        return result;
    }

    private async Task<IReadOnlyCollection<string>> GetJobGroupNamesAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await (
            from job in session.Query<Job>()
            group job by job.Group
            into jobGroups
            select jobGroups.Key
        ).ToListAsync(token).ConfigureAwait(false);
    }

    private async Task<IReadOnlyCollection<string>> GetTriggerGroupNamesAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await (
            from trigger in session.Query<Trigger>()
            group trigger by trigger.Group
            into triggerGroups
            select triggerGroups.Key
        ).ToListAsync(token).ConfigureAwait(false);
    }

    private async Task<IReadOnlyCollection<string>> GetCalendarNamesAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        return scheduler.ThrowIfNull().Calendars.Keys;
    }
    
    private async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(
        JobKey jobKey,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
            where trigger.JobKey == jobKey.GetDatabaseId()
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        return triggers.Select(x => x.Deserialize()).ToList();
    }

    private async Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (trigger == null) return TriggerState.None;

        return trigger.State switch
        {
            InternalTriggerState.Complete => TriggerState.Complete,
            InternalTriggerState.Blocked => TriggerState.Blocked,
            InternalTriggerState.PausedAndBlocked => TriggerState.Paused,
            InternalTriggerState.Paused => TriggerState.Paused,
            InternalTriggerState.Error => TriggerState.Error,
            _ => TriggerState.Normal
        };
    }

    private async Task ResetTriggerFromErrorStateAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (trigger is not { State: InternalTriggerState.Error }) return;

        var isTriggerGroupPaused = await IsTriggerGroupPausedAsync
        (
            session,
            trigger.Group,
            token
        ).ConfigureAwait(false);

        var scheduler = await session.LoadAsync<Scheduler>(InstanceName, token).ConfigureAwait(false);

        var isJobGroupPaused = scheduler.ThrowIfNull().PausedJobGroups.Contains(trigger.JobGroup);

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

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }

    private async Task PauseTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var trigger = await session
            .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), token)
            .ConfigureAwait(false);

        if (trigger == null || trigger.State == InternalTriggerState.Complete) return;

        trigger.State = trigger.State == InternalTriggerState.Blocked
            ? InternalTriggerState.PausedAndBlocked
            : InternalTriggerState.Paused;

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }

    private async Task<IReadOnlyCollection<string>> PauseTriggersAsync(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var result = new HashSet<string>();

        foreach (var trigger in triggers)
        {
            if (matcher.IsMatch(trigger.TriggerKey) == false) continue;
        
            trigger.State = trigger.State == InternalTriggerState.Blocked
                ? InternalTriggerState.PausedAndBlocked
                : InternalTriggerState.Paused;

            result.Add(trigger.TriggerKey.Group);
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);

        return result;
    }

    private async Task PauseJobAsync(JobKey jobKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
    }

    private async Task<IReadOnlyCollection<string>> PauseJobsAsync(
        GroupMatcher<JobKey> matcher,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var jobKeys = await (
            from job in session.Query<Job>()
            select job.JobKey
        ).ToListAsync(token).ConfigureAwait(false);

        var matchedJobKeys = new HashSet<string>();
        var result = new HashSet<string>();

        jobKeys.ForEach(x =>
        {
            if (matcher.IsMatch(x))
            {
                matchedJobKeys.Add(x.GetDatabaseId());
                result.Add(x.Group);
            }
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

        await session.SaveChangesAsync(token).ConfigureAwait(false);

        return result;
    }

    private async Task ResumeTriggerAsync(TriggerKey triggerKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
    }

    private async Task<IReadOnlyCollection<string>> ResumeTriggersAsync(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var triggers = await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.Scheduler)
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var result = new HashSet<string>();

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

        await session.SaveChangesAsync(token).ConfigureAwait(false);

        return result;
    }

    private async Task<IReadOnlyList<string>> GetPausedTriggerGroupsAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        return await GetPausedTriggerGroupsAsync(session, token).ConfigureAwait(false);
    }

    private async Task ResumeJobAsync(JobKey jobKey, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
    }

    private async Task ResumeAllTriggersAsync(CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        
        scheduler.PausedJobGroups.Clear();

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }

    private async Task<IReadOnlyCollection<string>> ResumeJobsAsync(
        GroupMatcher<JobKey> matcher,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var jobKeys = await (
            from job in session.Query<Job>()
                .Include(x => x.Scheduler)
            select job.JobKey
        ).ToListAsync(token).ConfigureAwait(false);

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        var matchedJobKeys = new HashSet<string>();
        var result = new HashSet<string>();

        jobKeys.ForEach(x =>
        {
            if (matcher.IsMatch(x))
            {
                matchedJobKeys.Add(x.GetDatabaseId());
                result.Add(x.Group);
            }
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

        await session.SaveChangesAsync(token).ConfigureAwait(false);

        return result;
    }

    private async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersAsync(
        DateTimeOffset noLaterThan,
        int maxCount,
        TimeSpan timeWindow,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

        var result = new List<IOperableTrigger>();
        var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

        var storedTriggers = await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.Scheduler)
                .Include(x => x.JobKey)
            where trigger.State == InternalTriggerState.Waiting
                  &&
                  trigger.NextFireTimeUtc <= (noLaterThan + timeWindow).UtcDateTime
            orderby trigger.NextFireTimeTicks,
                    trigger.Priority descending
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

        // Copy the list to another list, we may need to modify it.
        var candidateTriggers = new SortedSet<Trigger>(storedTriggers, new TriggerComparator());

        var scheduler = await session
            .LoadAsync<Scheduler>(InstanceName, token)
            .ConfigureAwait(false);

        while (candidateTriggers.Any() && result.Count < maxCount)
        {
            var trigger = candidateTriggers.Pop();
            if (trigger.NextFireTimeUtc == null) continue;
            
            var misfireApplied = await ApplyMisfireAsync(scheduler, trigger, token).ConfigureAwait(false);
            if (misfireApplied)
            {
                if (trigger.NextFireTimeUtc != null)
                {
                    candidateTriggers.Add(trigger);
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
                
                if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey)) continue;
                
                acquiredJobKeysForNoConcurrentExec.Add(jobKey);
            }

            trigger.State = InternalTriggerState.Acquired;
            trigger.FireInstanceId = GetFiredTriggerRecordId();

            result.Add(trigger.Deserialize());
        }

        return result;
    }

    private async Task ReleaseAcquiredTriggerAsync(IOperableTrigger trigger, CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        IReadOnlyCollection<IOperableTrigger> triggers,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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

            var calendar = scheduler.Calendars.TryGetValue(storedTrigger.CalendarName ?? string.Empty, out var entry)
                ? entry
                : null;

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
        IOperableTrigger trigger,
        IJobDetail jobDetail,
        SchedulerInstruction triggerInstCode,
        CancellationToken token)
    {
        using var session = DocumentStore.ThrowIfNull().OpenAsyncSession();

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
        }

        await session.SaveChangesAsync(token).ConfigureAwait(false);
    }
}
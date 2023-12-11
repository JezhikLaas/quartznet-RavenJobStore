using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Impl.RavenJobStore.Indexes;
using Quartz.Simpl;
using Quartz.Spi;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;

namespace Quartz.Impl.RavenJobStore;

public partial class RavenJobStore
{
    private IAsyncDocumentSession GetNonWaitingSession() =>
        DocumentStore.ThrowIfNull().OpenAsyncSession();
    
    private IAsyncDocumentSession GetSession()
    {
        var result = DocumentStore.ThrowIfNull().OpenAsyncSession();
        result.Advanced.OnBeforeQuery += AdvancedOnBeforeQuery;
        result.Advanced.OnSessionDisposing += AdvancedOnSessionDisposing;

        return result;
    }

    private static void AdvancedOnSessionDisposing(object? sender, SessionDisposingEventArgs e)
    {
        if (e.Session is not IAsyncDocumentSession session) return;
        
        session.Advanced.OnBeforeQuery -= AdvancedOnBeforeQuery;
        session.Advanced.OnSessionDisposing -= AdvancedOnSessionDisposing;
    }

    private static void AdvancedOnBeforeQuery(object? _, BeforeQueryEventArgs e)
    {
        e.QueryCustomization.WaitForNonStaleResults();
    }

    private async Task RestartTriggersForRecoveringJobsAsync(IAsyncDocumentSession session, CancellationToken token)
    {
        var recoveringJobKeys = await (
            from job in session.Query<Job>(nameof(JobIndex))
            where job.Scheduler == InstanceName && job.RequestsRecovery
            select job.Id
        ).ToListAsync(token).ConfigureAwait(false);

        var recoveringTriggers = await GetTriggersForJobKeysAsync
        (
            session,
            recoveringJobKeys,
            token
        ).ConfigureAwait(false);

        foreach (var trigger in recoveringTriggers)
        {
            var operableTrigger = trigger.Item.ThrowIfNull();
            operableTrigger.ComputeFirstFireTimeUtc(null);

            trigger.NextFireTimeUtc = operableTrigger.GetNextFireTimeUtc();
        }
    }

    private static async Task DeleteCompletedTriggersAsync(
        IAsyncDocumentSession session,
        IEnumerable<Trigger> triggers,
        CancellationToken token)
    {
        var completedTriggers = triggers
            .Where
            (
                x => x.State is InternalTriggerState.Complete
            )
            .ToList();

        var jobKeys = completedTriggers.Select(x => x.JobId);
        var jobs = await session.LoadAsync<Job>(jobKeys, token);
        var existingJobKeys = jobs
            .Where(x => x.Value != null)
            .Select(x => x.Key)
            .ToList();

        var triggersForJobs = await GetTriggersForJobKeysAsync
        (
            session,
            existingJobKeys,
            token
        ).ConfigureAwait(false);

        foreach (var trigger in completedTriggers)
        {
            if (jobs.TryGetValue(trigger.JobId, out var job))
            {
                // We got a job for the completed trigger.
                if (job.Durable == false)
                {
                    // The job is not durable and may be deleted.
                    if (triggersForJobs.Any(x => x.Id != trigger.Id && x.JobId == job.Id) == false)
                    {
                        // There is no other trigger than the current one
                        // referencing this job, so it has to be deleted.
                        session.Delete(job);
                    }
                }
            }
            
            session.Delete(trigger.Id);
        }
    }

    private static void ResetInterruptedTriggers(IEnumerable<Trigger> triggers)
    {
        var interruptedTriggers = triggers
            .Where
            (
                x => x.State is InternalTriggerState.Acquired or InternalTriggerState.Blocked
            );
        
        foreach (var trigger in interruptedTriggers)
        {
            trigger.State = InternalTriggerState.Waiting;
        }
    }

    private static string GetFiredTriggerRecordId()
    {
        var value = Interlocked.Increment(ref _fireTimeCounter);
        return $"{value:D19}";
    }

    private async Task SetAllTriggersOfJobToStateAsync(
        IAsyncDocumentSession session,
        JobKey jobKey,
        InternalTriggerState state,
        CancellationToken token)
    {
        var jobId = jobKey.GetDatabaseId(InstanceName);
        
        var triggersForJob = await (
            from item in session.Query<Trigger>()
            where item.JobId == jobId
            select item
        ).ToListAsync(token).ConfigureAwait(false);

        foreach (var trigger in triggersForJob)
        {
            trigger.State = state;
        }
    }

    private static async Task<IReadOnlyList<Trigger>> GetTriggersForJobKeysAsync(
        IAsyncDocumentSession session,
        IReadOnlyList<string> jobKeys,
        CancellationToken token) =>
        await (
            from trigger in session.Query<Trigger>()
                .Include(x => x.CalendarId)
            where trigger.JobId.In(jobKeys)
            select trigger
        ).ToListAsync(token).ConfigureAwait(false);

    private async Task<bool> ApplyMisfireAsync(IAsyncDocumentSession session, Trigger trigger, CancellationToken token)
    {
        var misfireTime = SystemTime.UtcNow();
        if (MisfireThreshold > TimeSpan.Zero)
        {
            misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
        }

        var fireTimeUtc = trigger.NextFireTimeUtc;
        if (!fireTimeUtc.HasValue || fireTimeUtc.Value > misfireTime
                                  || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            return false;

        var calendar = await session
            .LoadAsync<Entities.Calendar>(trigger.CalendarId, token)
            .ConfigureAwait(false);

        var operableTrigger = trigger.Item.ThrowIfNull();
        await Signaler.NotifyTriggerListenersMisfired(operableTrigger, token);
        
        operableTrigger.UpdateAfterMisfire(calendar?.Item);
        trigger.Item = operableTrigger;

        if (operableTrigger.GetNextFireTimeUtc().HasValue == false)
        {
            await Signaler.NotifySchedulerListenersFinalized(operableTrigger, token);
            trigger.State = InternalTriggerState.Complete;
        }
        else if (fireTimeUtc.Equals(operableTrigger.GetNextFireTimeUtc()))
        {
            return false;
        }

        return true;
    }

    internal async Task RecoverJobStoreAsync(IAsyncDocumentSession session, CancellationToken token)
    {
        try
        {
            // Fetch all triggers which seem to be in an inconsistent state.
            var inconsistentTriggers = await (
                from trigger in session.Query<Trigger>()
                    .Include(x => x.Scheduler)
                where trigger.Scheduler == InstanceName
                      &&
                      (
                          trigger.State == InternalTriggerState.Acquired
                          ||
                          trigger.State == InternalTriggerState.Blocked
                          ||
                          trigger.State == InternalTriggerState.Complete
                      )
                select trigger
            ).ToListAsync(token).ConfigureAwait(false);

            ResetInterruptedTriggers(inconsistentTriggers);

            await UnblockBlockedJobsAsync(session, token).ConfigureAwait(false);
            await DeleteCompletedTriggersAsync(session, inconsistentTriggers, token).ConfigureAwait(false);
            await RestartTriggersForRecoveringJobsAsync(session, token).ConfigureAwait(false);

            await session.SaveChangesAsync(token).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            throw new JobPersistenceException("Couldn't recover jobs: " + error.Message, error);
        }
    }

    private Task<bool> IsTriggerGroupPausedAsync(
        IAsyncDocumentSession session,
        string groupName,
        CancellationToken token)
    {
        var groupId = PausedTriggerGroup.GetId(InstanceName, groupName);
        return session.Advanced.ExistsAsync(groupId, token);
    }

    private Task<bool> IsJobGroupPausedAsync(
        IAsyncDocumentSession session,
        string groupName,
        CancellationToken token)
    {
        var groupId = PausedJobGroup.GetId(InstanceName, groupName);
        return session.Advanced.ExistsAsync(groupId, token);
    }

    private async Task<IReadOnlyList<string>> GetPausedTriggerGroupsAsync(
        IAsyncDocumentSession session,
        CancellationToken token) =>
        await (
            from entity in session.Query<PausedTriggerGroup>(nameof(PausedTriggerGroupIndex))
            where entity.Scheduler == InstanceName
            select entity.GroupName
        ).ToListAsync(token).ConfigureAwait(false);

    private async Task<IReadOnlyList<string>> GetPausedJobGroupsAsync(
        IAsyncDocumentSession session,
        CancellationToken token) =>
        await (
            from entity in session.Query<PausedJobGroup>(nameof(PausedJobGroupIndex))
            where entity.Scheduler == InstanceName
            select entity.GroupName
        ).ToListAsync(token);

    private async Task EnsurePausedTriggerGroupAsync(
        IAsyncDocumentSession session,
        string group,
        CancellationToken token)
    {
        if (await IsTriggerGroupPausedAsync(session, group, token).ConfigureAwait(false)) return;
        await session
            .StoreAsync(new PausedTriggerGroup(InstanceName, group), token)
            .ConfigureAwait(false);
    }

    private async Task EnsurePausedJobGroupAsync(
        IAsyncDocumentSession session,
        string group,
        CancellationToken token)
    {
        if (await IsJobGroupPausedAsync(session, group, token).ConfigureAwait(false)) return;
        await session
            .StoreAsync(new PausedJobGroup(InstanceName, group), token)
            .ConfigureAwait(false);
    }

    private async Task<IReadOnlyCollection<string>> GetTriggerGroupNamesAsync(
        IAsyncDocumentSession session,
        CancellationToken token) =>
        await (
            from trigger in session.Query<TriggerGroupsIndex.Result>(nameof(TriggerGroupsIndex))
            where trigger.Scheduler == InstanceName
            select trigger.Group
        ).ToListAsync(token).ConfigureAwait(false);

    private async Task<Trigger> CreateConfiguredTriggerAsync(
        IOperableTrigger newTrigger,
        IAsyncDocumentSession session,
        CancellationToken token)
    {
        var trigger = new Trigger(newTrigger, InstanceName);

        var isTriggerGroupPaused = await IsTriggerGroupPausedAsync
        (
            session,
            trigger.Group,
            token
        ).ConfigureAwait(false);

        var isJobGroupPaused = await IsJobGroupPausedAsync
        (
            session,
            trigger.JobGroup,
            token
        ).ConfigureAwait(false);

        var isJobBlocked = await IsJobBlockedAsync
        (
            session,
            newTrigger.JobKey.GetDatabaseId(InstanceName),
            token
        ).ConfigureAwait(false);
        
        if (isTriggerGroupPaused || isJobGroupPaused)
        {
            trigger.State = InternalTriggerState.Paused;

            if (isJobBlocked)
            {
                trigger.State = InternalTriggerState.PausedAndBlocked;
            }
        }
        else if (isJobBlocked)
        {
            trigger.State = InternalTriggerState.Blocked;
        }
        
        return trigger;
    }

    private Task<bool> IsJobBlockedAsync(IAsyncDocumentSession session, string jobId, CancellationToken token) =>
        session.Advanced.ExistsAsync(BlockedJob.GetId(InstanceName, jobId), token);

    internal async Task<IReadOnlyList<string>> GetBlockedJobsAsync
    (
        IAsyncDocumentSession session,
        CancellationToken token
    ) =>
        await (
            from blocked in session.Query<BlockedJob>(nameof(BlockedJobIndex))
            where blocked.Scheduler == InstanceName
            select blocked.JobId
        ).ToListAsync(token).ConfigureAwait(false);

    private async Task UnblockBlockedJobsAsync(IAsyncDocumentSession session, CancellationToken token)
    {
        var ids = await (
            from blocked in session.Query<BlockedJob>(nameof(BlockedJobIndex))
            where blocked.Scheduler == InstanceName
            select blocked.Id
        ).ToListAsync(token).ConfigureAwait(false);
        
        ids.ForEach(session.Delete);
    }

    private async Task DeleteJobIfSingleReferenceAsync(IAsyncDocumentSession session, string jobId, string triggerId)
    {
        var otherTriggers = await (
            from trigger in session.Query<Trigger>(nameof(TriggerIndex))
            where trigger.Scheduler == InstanceName
                  &&
                  trigger.JobId == jobId
                  &&
                  trigger.Id != triggerId
            select trigger
        ).AnyAsync().ConfigureAwait(false);
        
        if (otherTriggers == false) session.Delete(jobId);
    }

    
    private async Task GetFiringCandidatesAsync(
        IAsyncDocumentSession session,
        PriorityQueue<Trigger, int> buffer,
        DateTimeOffset upperLimit,
        int skip,
        int count,
        CancellationToken token)
    {
        var result = await (
            from trigger in session.Query<Trigger>(nameof(TriggerIndex))
                .Include(x => x.CalendarId)
                .Include(x => x.JobId)
            where trigger.Scheduler == InstanceName
                  &&
                  trigger.State == InternalTriggerState.Waiting
                  &&
                  trigger.NextFireTimeUtc <= upperLimit
            orderby trigger.NextFireTimeUtc, trigger.Priority descending
            select trigger
        ).Skip(skip).Take(count).ToListAsync(token).ConfigureAwait(false);
        
        result.ForEach(x => buffer.Enqueue(x, -x.Priority));
    }

    private void WaitForIndexing()
    {
        var operationExecutor = DocumentStore!.Maintenance.ForDatabase(DocumentStore!.Database);
        var timeout = TimeSpan.FromMinutes(1.0);
        var stopwatch = Stopwatch.StartNew();
        
        while (stopwatch.Elapsed < timeout)
        {
            var databaseStatistics = operationExecutor.Send(new GetStatisticsOperation());
            var done = databaseStatistics
                .Indexes
                .Where<IndexInformation>(x => x.State != IndexState.Disabled)
                .All
                (
                    (Func<IndexInformation, bool>)(x => x.IsStale == false
                                                        &&
                                                        x.Name.StartsWith("ReplacementOf/") == false)
                );
            
            if (done) return;

            if (databaseStatistics.Indexes.All(x => x.State != IndexState.Error))
            {
                Thread.Sleep(100);
            }
            else break;
        }
    }
    
    private async Task RetryConcurrencyConflictAsync(Func<Task> action)
    {
        var counter = 100;
        
        while (counter-- > 0)
        {
            try
            {
                await action().ConfigureAwait(false);
                return;
            }
            catch (ConcurrencyException error)
            {
                if (counter <= 0)
                {

                    LogUnresolvableConcurrencyProblem(Logger, error);
                    throw;
                }

                LogConcurrencyProblem(Logger, error.Message);
            }
        }
    }

    private async Task<T> RetryConcurrencyConflictAsync<T>(Func<Task<T>> action)
    {
        var counter = 100;
        
        while (counter-- > 0)
        {
            try
            {
                return await action().ConfigureAwait(false);
            }
            catch (ConcurrencyException error)
            {
                if (counter <= 0)
                {

                    LogUnresolvableConcurrencyProblem(Logger, error);
                    throw;
                }

                LogConcurrencyProblem(Logger, error.Message);
            }
        }

        throw new Exception("Should never go here");
    }

    private partial void NotifyDebugWatcher(SchedulerExecutionStep step);
    
    #if DEBUG
    private partial void NotifyDebugWatcher(SchedulerExecutionStep step)
    {
        DebugWatcher?.Notify(step, InstanceId);
    }
    #endif

    [LoggerMessage(Level = LogLevel.Information, EventId = 1, Message = "Concurrency problem: {error}")]
    public static partial void LogConcurrencyProblem(ILogger logger, string error);

    [LoggerMessage(Level = LogLevel.Information, EventId = 2, Message = "Could not recover from concurrency problem")]
    public static partial void LogUnresolvableConcurrencyProblem(ILogger logger, Exception error);

    [LoggerMessage(Level = LogLevel.Trace, EventId = 3, Message = "Enter {name}")]
    public static partial void TraceEnter(ILogger logger, [CallerMemberName]string? name = null);

    [LoggerMessage(Level = LogLevel.Trace, EventId = 4, Message = "Exit {name}")]
    public static partial void TraceExit(ILogger logger, [CallerMemberName]string? name = null);

    [LoggerMessage(Level = LogLevel.Trace, EventId = 5, Message = "Exit {name} with {result}")]
    public static partial void TraceExit(ILogger logger, object? result, [CallerMemberName]string? name = null);
}

public interface IDebugWatcher
{
    void Notify(SchedulerExecutionStep step, string instanceId);
}
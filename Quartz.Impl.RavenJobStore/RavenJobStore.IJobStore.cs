using Quartz.Impl.Matchers;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Spi;

namespace Quartz.Impl.RavenJobStore;

/// <inheritdoc />
public partial class RavenJobStore : IJobStore
{
    /// <inheritdoc />
    public Task Initialize(
        ITypeLoadHelper loadHelper,
        ISchedulerSignaler signaler,
        CancellationToken cancellationToken = default)
    {
        Signaler = signaler;
        DocumentStore ??= InitializeDocumentStore();

        return EnsureIndexesAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task SchedulerStarted(CancellationToken cancellationToken = new()) => 
        SchedulerStartedAsync(cancellationToken);

    /// <inheritdoc />
    public Task SchedulerPaused(CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => SetSchedulerStateAsync(SchedulerState.Paused, cancellationToken)
        );

    /// <inheritdoc />
    public Task SchedulerResumed(CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => SetSchedulerStateAsync(SchedulerState.Resumed, cancellationToken)
        );

    /// <inheritdoc />
    public Task Shutdown(CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => SetSchedulerStateAsync(SchedulerState.Shutdown, cancellationToken)
        );

    /// <inheritdoc />
    public Task StoreJobAndTrigger(
        IJobDetail newJob,
        IOperableTrigger newTrigger,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => StoreJobAndTriggerAsync(newJob, newTrigger, cancellationToken)
        );

    /// <inheritdoc />
    public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = new()) => 
        IsJobGroupPausedAsync(groupName, cancellationToken);

    /// <inheritdoc />
    public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = new()) => 
        IsTriggerGroupPausedAsync(groupName, cancellationToken);

    /// <inheritdoc />
    public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => StoreJobAsync(newJob, replaceExisting, cancellationToken)
        );

    /// <inheritdoc />
    public Task StoreJobsAndTriggers(
        IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
        bool replace,
        CancellationToken cancellationToken = new()) =>
        StoreJobsAndTriggersAsync
        (
            triggersAndJobs,
            replace,
            cancellationToken
        );

    /// <inheritdoc />
    public Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => RemoveJobAsync(jobKey, cancellationToken)
        );

    /// <inheritdoc />
    public Task<bool> RemoveJobs(
        IReadOnlyCollection<JobKey> jobKeys,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => RemoveJobsAsync(jobKeys, cancellationToken)
        );

    /// <inheritdoc />
    public Task<IJobDetail?> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = new()) => 
        RetrieveJobAsync(jobKey, cancellationToken);

    /// <inheritdoc />
    public Task StoreTrigger(
        IOperableTrigger newTrigger,
        bool replaceExisting,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => StoreTriggerAsync(newTrigger, replaceExisting, cancellationToken)
        );

    /// <inheritdoc />
    public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => RemoveTriggerAsync(triggerKey, cancellationToken)
        );

    /// <inheritdoc />
    public Task<bool> RemoveTriggers(
        IReadOnlyCollection<TriggerKey> triggerKeys,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => RemoveTriggersAsync(triggerKeys, cancellationToken)
        );

    /// <inheritdoc />
    public Task<bool> ReplaceTrigger(
        TriggerKey triggerKey,
        IOperableTrigger newTrigger,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ReplaceTriggerAsync(triggerKey, newTrigger, cancellationToken)
        );

    /// <inheritdoc />
    public Task<IOperableTrigger?> RetrieveTrigger(
        TriggerKey triggerKey,
        CancellationToken cancellationToken = new()) =>
        RetrieveTriggerAsync(triggerKey, cancellationToken);

    /// <inheritdoc />
    public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = new()) => 
        CalendarExistsAsync(calName, cancellationToken);

    /// <inheritdoc />
    public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = new()) => 
        CheckExistsAsync(jobKey, cancellationToken);

    /// <inheritdoc />
    public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = new()) => 
        CheckExistsAsync(triggerKey, cancellationToken);

    /// <inheritdoc />
    public Task ClearAllSchedulingData(CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ClearAllSchedulingDataAsync(cancellationToken)
        );

    /// <inheritdoc />
    public Task StoreCalendar(
        string name, 
        ICalendar calendar,
        bool replaceExisting,
        bool updateTriggers,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => StoreCalendarAsync
            (
                name,
                calendar,
                replaceExisting,
                updateTriggers,
                cancellationToken
            )
        );

    /// <inheritdoc />
    public Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => RemoveCalendarAsync(calName, cancellationToken)
        );

    /// <inheritdoc />
    public Task<ICalendar?> RetrieveCalendar(string calName, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => RetrieveCalendarAsync(calName, cancellationToken)
        );

    /// <inheritdoc />
    public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = new()) => 
        GetNumberOfJobsAsync(cancellationToken);

    /// <inheritdoc />
    public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = new()) => 
        GetNumberOfTriggersAsync(cancellationToken);

    /// <inheritdoc />
    public Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = new()) => 
        GetNumberOfCalendarsAsync(cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyCollection<JobKey>> GetJobKeys(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = new()) =>
        GetJobKeysAsync(matcher, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken cancellationToken = new()) =>
        GetTriggerKeysAsync(matcher, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = new()) => 
        GetJobGroupNamesAsync(cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = new()) => 
        GetTriggerGroupNamesAsync(cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = new()) => 
        GetCalendarNamesAsync(cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
        JobKey jobKey,
        CancellationToken cancellationToken = new()) =>
        GetTriggersForJobAsync(jobKey, cancellationToken);

    /// <inheritdoc />
    public Task<TriggerState> GetTriggerState(
        TriggerKey triggerKey,
        CancellationToken cancellationToken = new()) =>
        GetTriggerStateAsync(triggerKey, cancellationToken);

    /// <inheritdoc />
    public Task ResetTriggerFromErrorState(TriggerKey triggerKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ResetTriggerFromErrorStateAsync(triggerKey,cancellationToken)
        );

    /// <inheritdoc />
    public Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => PauseTriggerAsync(triggerKey, cancellationToken)
        );

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> PauseTriggers(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => PauseTriggersAsync(matcher, cancellationToken)
        );

    /// <inheritdoc />
    public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => PauseJobAsync(jobKey, cancellationToken)
        );

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> PauseJobs(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => PauseJobsAsync(matcher, cancellationToken)
        );

    /// <inheritdoc />
    public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ResumeTriggerAsync(triggerKey, cancellationToken)
        );

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> ResumeTriggers(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ResumeTriggersAsync(matcher, cancellationToken)
        );

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
        CancellationToken cancellationToken = new()) => 
        await GetPausedTriggerGroupsAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ResumeJobAsync(jobKey, cancellationToken)
        );

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> ResumeJobs(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ResumeJobsAsync(matcher, cancellationToken)
        );

    /// <inheritdoc />
    public Task PauseAll(CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => PauseTriggersAsync(GroupMatcher<TriggerKey>.AnyGroup(), cancellationToken)
        );

    /// <inheritdoc />
    public Task ResumeAll(CancellationToken cancellationToken = new()) =>
        RetryConcurrencyConflictAsync
        (
            () => ResumeAllTriggersAsync(cancellationToken)
        );

    /// <inheritdoc />
    public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
        DateTimeOffset noLaterThan,
        int maxCount,
        TimeSpan timeWindow,
        CancellationToken cancellationToken = new())
    {
        NotifyDebugWatcher(SchedulerExecutionStep.Acquiring);
        return RetryConcurrencyConflictAsync
        (
            () => AcquireNextTriggersAsync
            (
                noLaterThan,
                maxCount,
                timeWindow,
                cancellationToken
            )
        );
    }

    /// <inheritdoc />
    public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = new())
    {
        NotifyDebugWatcher(SchedulerExecutionStep.Releasing);
        return RetryConcurrencyConflictAsync
        (
            () => ReleaseAcquiredTriggerAsync(trigger, cancellationToken)
        );
    }

    /// <inheritdoc />
    public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
        IReadOnlyCollection<IOperableTrigger> triggers,
        CancellationToken cancellationToken = new())
    {
        NotifyDebugWatcher(SchedulerExecutionStep.Firing);
        return RetryConcurrencyConflictAsync
        (
            () => TriggersFiredAsync(triggers, cancellationToken)
        );
    }

    /// <inheritdoc />
    public Task TriggeredJobComplete(
        IOperableTrigger trigger,
        IJobDetail jobDetail,
        SchedulerInstruction triggerInstCode,
        CancellationToken cancellationToken = new())
    {
        NotifyDebugWatcher(SchedulerExecutionStep.Completing);
        return RetryConcurrencyConflictAsync
        (
            () => TriggeredJobCompleteAsync
            (
                trigger,
                jobDetail,
                triggerInstCode,
                cancellationToken
            )
        );
    }
}
using Quartz.Impl.Matchers;
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

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task SchedulerStarted(CancellationToken cancellationToken = new()) => 
        await SchedulerStartedAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task SchedulerPaused(CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            SetSchedulerStateAsync(SchedulerState.Paused, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SchedulerResumed(CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            SetSchedulerStateAsync(SchedulerState.Resumed, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task Shutdown(CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            SetSchedulerStateAsync(SchedulerState.Shutdown, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task StoreJobAndTrigger(
        IJobDetail newJob,
        IOperableTrigger newTrigger,
        CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            StoreJobAndTriggerAsync(newJob, newTrigger, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = new()) => 
        await IsJobGroupPausedAsync(groupName, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = new()) => 
        await IsTriggerGroupPausedAsync(groupName, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            StoreJobAsync(newJob, replaceExisting, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task StoreJobsAndTriggers(
        IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
        bool replace,
        CancellationToken cancellationToken = new())
    {
        await StoreJobsAndTriggersAsync
        (
            triggersAndJobs,
            replace,
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            RemoveJobAsync(jobKey, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            RemoveJobsAsync(jobKeys, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IJobDetail?> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = new()) => 
        await RetrieveJobAsync(jobKey, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task StoreTrigger(
        IOperableTrigger newTrigger,
        bool replaceExisting,
        CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            StoreTriggerAsync(newTrigger, replaceExisting, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            RemoveTriggerAsync(triggerKey, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveTriggers(
        IReadOnlyCollection<TriggerKey> triggerKeys,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            RemoveTriggersAsync(triggerKeys, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ReplaceTrigger(
        TriggerKey triggerKey,
        IOperableTrigger newTrigger,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            ReplaceTriggerAsync(triggerKey, newTrigger, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IOperableTrigger?> RetrieveTrigger(
        TriggerKey triggerKey,
        CancellationToken cancellationToken = new()) =>
        await RetrieveTriggerAsync(triggerKey, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = new()) => 
        await CalendarExistsAsync(calName, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = new()) => 
        await CheckExistsAsync(jobKey, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = new()) => 
        await CheckExistsAsync(triggerKey, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task ClearAllSchedulingData(CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            ClearAllSchedulingDataAsync(cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task StoreCalendar(
        string name, 
        ICalendar calendar,
        bool replaceExisting,
        bool updateTriggers,
        CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            StoreCalendarAsync
            (
                name,
                calendar,
                replaceExisting,
                updateTriggers,
                cancellationToken
            )
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            RemoveCalendarAsync(calName, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<ICalendar?> RetrieveCalendar(string calName, CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            RetrieveCalendarAsync(calName, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = new()) => 
        await GetNumberOfJobsAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = new()) => 
        await GetNumberOfTriggersAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = new()) => 
        await GetNumberOfCalendarsAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = new()) =>
        await GetJobKeysAsync(matcher, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken cancellationToken = new()) =>
        await GetTriggerKeysAsync(matcher, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = new()) => 
        await GetJobGroupNamesAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = new()) => 
        await GetTriggerGroupNamesAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = new()) => 
        await GetCalendarNamesAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
        JobKey jobKey,
        CancellationToken cancellationToken = new()) =>
        await GetTriggersForJobAsync(jobKey, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<TriggerState> GetTriggerState(
        TriggerKey triggerKey,
        CancellationToken cancellationToken = new()) =>
        await GetTriggerStateAsync(triggerKey, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task ResetTriggerFromErrorState(TriggerKey triggerKey, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            ResetTriggerFromErrorStateAsync(triggerKey,cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            PauseTriggerAsync(triggerKey, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> PauseTriggers(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            PauseTriggersAsync(matcher, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            PauseJobAsync(jobKey, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> PauseJobs(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            PauseJobsAsync(matcher, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            ResumeTriggerAsync(triggerKey, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> ResumeTriggers(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            ResumeTriggersAsync(matcher, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
        CancellationToken cancellationToken = new()) => 
        await GetPausedTriggerGroupsAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            ResumeJobAsync(jobKey, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> ResumeJobs(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            ResumeJobsAsync(matcher, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task PauseAll(CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            PauseTriggersAsync(GroupMatcher<TriggerKey>.AnyGroup(), cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ResumeAll(CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            ResumeAllTriggersAsync(cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
        DateTimeOffset noLaterThan,
        int maxCount,
        TimeSpan timeWindow,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            AcquireNextTriggersAsync
            (
                noLaterThan,
                maxCount,
                timeWindow,
                cancellationToken
            )
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            ReleaseAcquiredTriggerAsync(trigger, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
        IReadOnlyCollection<IOperableTrigger> triggers,
        CancellationToken cancellationToken = new())
    {
        return await RetryConcurrencyConflictAsync
        (
            TriggersFiredAsync(triggers, cancellationToken)
        ).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task TriggeredJobComplete(
        IOperableTrigger trigger,
        IJobDetail jobDetail,
        SchedulerInstruction triggerInstCode,
        CancellationToken cancellationToken = new())
    {
        await RetryConcurrencyConflictAsync
        (
            TriggeredJobCompleteAsync
            (
                trigger,
                jobDetail,
                triggerInstCode,
                cancellationToken
            )
        ).ConfigureAwait(false);
    }
}
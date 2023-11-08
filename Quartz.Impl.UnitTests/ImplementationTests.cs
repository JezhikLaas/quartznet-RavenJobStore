using FluentAssertions;
using Quartz.Impl.Triggers;
using Quartz.Simpl;

namespace Quartz.Impl.UnitTests;

using RavenJobStore;

public class ImplementationTests : TestBase
{
    private RavenJobStore Target { get; }

    public ImplementationTests()
    {
        var store = CreateStore();
        Target = new RavenJobStore(store);
    }

    public override void Dispose()
    {
        Target.DocumentStore!.Dispose();
        base.Dispose();
    }

    [Fact(DisplayName = "If no scheduler exists Then SchedulerStarted creates one")]
    public async Task If_no_scheduler_exists_Then_SchedulerStarted_creates_one()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var scheduler = await session.LoadAsync<Scheduler>(Target.InstanceName);

        scheduler.Should().NotBeNull();
    }

    [Fact(DisplayName = "If no scheduler exists Then SetSchedulerState throws")]
    public async Task If_no_scheduler_exists_Then_SetSchedulerState_throws()
    {
        var call = () => Target.SetSchedulerStateAsync(SchedulerState.Paused, CancellationToken.None);
        await call.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact(DisplayName = "If a scheduler exists Then SetScheduler State sets the requested state")]
    public async Task If_a_scheduler_exists_Then_SetScheduler_State_sets_the_requested_state()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        await Target.SetSchedulerStateAsync(SchedulerState.Paused, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var scheduler = await session.LoadAsync<Scheduler>(Target.InstanceName);

        scheduler.State.Should().Be(SchedulerState.Paused);
    }

    [Fact(DisplayName = "If StoreJobAndTrigger is called on a valid store Then job and trigger are created")]
    public async Task If_StoreJobAndTrigger_is_called_on_a_valid_store_Then_job_and_trigger_are_created()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        await Target.StoreJobAndTriggerAsync(job, trigger, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJob = await session.LoadAsync<Job>(job.Key.GetDatabaseId());
        var checkTrigger = await session.LoadAsync<Trigger>(trigger.Key.GetDatabaseId());

        checkJob.Should().NotBeNull();
        checkTrigger.Should().NotBeNull();
    }

    [Fact(DisplayName = "If requested group is unknown Then IsJobGroupPaused returns false")]
    public async Task If_requested_group_is_unknown_Then_IsJobGroupPaused_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        var result = await Target.IsJobGroupPausedAsync("unknown", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If requested group is known Then IsJobGroupPaused returns true")]
    public async Task If_requested_group_is_known_Then_IsJobGroupPaused_returns_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
 
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var scheduler = await session.LoadAsync<Scheduler>(Target.InstanceName);

        scheduler.PausedJobGroups.Add("known");
        await session.SaveChangesAsync();

        var result = await Target.IsJobGroupPausedAsync("known", CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If no scheduler exists Then IsJobGroupPaused throws")]
    public async Task If_no_scheduler_exists_Then_IsJobGroupPaused_throws()
    {
        var call = () => Target.IsJobGroupPausedAsync("unknown", CancellationToken.None);
        await call.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact(DisplayName = "If requested group is unknown Then IsTriggerGroupPaused returns false")]
    public async Task If_requested_group_is_unknown_Then_IsTriggerGroupPaused_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        var result = await Target.IsTriggerGroupPausedAsync("unknown", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If requested group is known Then IsTriggerGroupPaused returns true")]
    public async Task If_requested_group_is_known_Then_IsTriggerGroupPaused_returns_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
 
        using var session = Target.DocumentStore!.OpenAsyncSession();
        await session.StoreAsync
        (
            new Trigger(new SimpleTriggerImpl("X", "known"), Target.InstanceName)
            {
                State = InternalTriggerState.Paused
            }
        );
        
        await session.SaveChangesAsync();

        var result = await Target.IsTriggerGroupPausedAsync("known", CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If no scheduler exists Then IsTriggerGroupPaused does not throw")]
    public async Task If_no_scheduler_exists_Then_IsTriggerGroupPaused_does_not_throw()
    {
        var call = () => Target.IsTriggerGroupPausedAsync("unknown", CancellationToken.None);
        await call.Should().NotThrowAsync();
    }
}
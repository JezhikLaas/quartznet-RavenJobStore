using FluentAssertions;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Impl.RavenJobStore.UnitTests.Helpers;
using Quartz.Impl.RavenJobStore.UnitTests.Jobs;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Xunit.Abstractions;

namespace Quartz.Impl.RavenJobStore.UnitTests;

public class UtilsTests : TestBase
{
    private RavenJobStore Target { get; }
    
    private ITestOutputHelper Output { get; }

    public UtilsTests(ITestOutputHelper output)
    {
        var store = CreateStore();
        
        Output = output;
        Target = new RavenJobStore(store)
        {
            Logger = Output.BuildLoggerFor<RavenJobStore>()
        };
    }

    public override void Dispose()
    {
        Target.DocumentStore!.Dispose();
        base.Dispose();
        
        GC.SuppressFinalize(this);
    }

    [Theory(DisplayName = "If interrupted triggers exist Then RecoverJobStore puts them to waiting")]
    [InlineData(InternalTriggerState.Acquired)]
    [InlineData(InternalTriggerState.Blocked)]
    public async Task If_interrupted_triggers_exist_Then_RecoverJobStore_puts_them_to_waiting(
        InternalTriggerState given)
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        
        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var storedTrigger = await session.LoadAsync<Trigger>(trigger.Key.GetDatabaseId(Target.InstanceName));
            storedTrigger.State = given;

            await session.SaveChangesAsync();
        }

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            await Target.RecoverJobStoreAsync(session, CancellationToken.None);
        }

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var storedTrigger = await session.LoadAsync<Trigger>(trigger.Key.GetDatabaseId(Target.InstanceName));
            storedTrigger.State.Should().Be(InternalTriggerState.Waiting);
        }
    }
}
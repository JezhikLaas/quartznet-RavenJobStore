using FakeItEasy;
using FluentAssertions;
using Quartz.Impl.Matchers;
using Quartz.Impl.RavenJobStore;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Impl.UnitTests.Helpers;
using Quartz.Impl.UnitTests.Jobs;
using Quartz.Spi;

namespace Quartz.Impl.UnitTests;

public class SingleSchedulerTests : SchedulerTestBase
{
    private IScheduler? Scheduler { get; set; }

    public override void Dispose()
    {
        Scheduler?.Shutdown();
        base.Dispose();
        
        GC.SuppressFinalize(this);
    }
    
    [Fact(DisplayName = "If a scheduler is created with type raven Then a RavenJobStore gets instantiated")]
    public async Task If_a_scheduler_is_created_with_type_raven_Then_a_RavenJobStore_gets_instantiated()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test");
        await Scheduler.Start();
        
        var ravenJobStore = GetStore(Scheduler);
        ravenJobStore.Should().NotBeNull();
    }
    
    [Fact(DisplayName = "If a collection name is used Then documents are placed within it")]
    public async Task If_a_collection_name_is_used_Then_documents_are_placed_within_it()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();
        
        using var session = DocumentStore.OpenAsyncSession();
        var scheduler = await session.LoadAsync<Scheduler>("Test");
        var meta = session.Advanced.GetMetadataFor(scheduler);

        meta.Should().Contain(x => x.Key == "@collection" && x.Value.Equals("SchedulerData/Schedulers"));
    }

    [Fact(DisplayName = "If a DebugWatcher is set Then it gets notified")]
    public async Task If_a_DebugWatcher_is_set_Then_it_gets_notified()
    {
        var watcher = A.Fake<IDebugWatcher>();
        
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();
        
        var store = GetStore(Scheduler); 
        store.DebugWatcher = watcher;

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger1", "Group")
            .StartNow()
            .WithDescription("Unexpected")
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Scheduler.ScheduleJob(job, triggerOne, CancellationToken.None);

        var existingJobs = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());
        while (existingJobs.Any())
        {
            await Task.Delay(50);
            existingJobs = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());
        }

        A.CallTo(() => watcher.Notify(SchedulerExecutionStep.Acquiring, A<string>._))
            .MustHaveHappened();
        A.CallTo(() => watcher.Notify(SchedulerExecutionStep.Firing, A<string>._))
            .MustHaveHappenedOnceExactly();
        A.CallTo(() => watcher.Notify(SchedulerExecutionStep.Completing, A<string>._))
            .MustHaveHappenedOnceExactly();
    }
    
    [Fact(DisplayName = "If a collection name is used Then documents are placed within it")]
    public async Task If_a_collection_name_is_used_Then_job_keys_are_correctly_fetched()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();
        
        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger1", "Group")
            .StartAt(DateTimeOffset.UtcNow.AddHours(5))
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Scheduler.ScheduleJob(job, triggerOne, CancellationToken.None);

        var result = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());

        result.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Name == "Job" && x.Group == "Group");
    }
}
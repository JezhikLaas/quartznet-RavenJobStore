using Domla.Quartz.Raven;
using Domla.Quartz.Raven.Entities;
using FakeItEasy;
using FluentAssertions;
using Quartz.Impl.Matchers;
using Quartz.Impl.RavenJobStore.UnitTests.Helpers;
using Quartz.Impl.RavenJobStore.UnitTests.Jobs;
using Quartz.Spi;
using Raven.Client.Exceptions;

namespace Quartz.Impl.RavenJobStore.UnitTests;

[Collection("DB")]
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

    [Fact(DisplayName = "If a job uses persistent data Then it is updated during completion")]
    public async Task If_a_job_uses_persistent_data_Then_it_is_updated_during_completion()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();

        var watcher = new ControllingWatcher(Scheduler.SchedulerInstanceId, SchedulerExecutionStep.Completed);

        var store = GetStore(Scheduler);
        store.DebugWatcher = watcher;
        
        var job = JobBuilder
            .Create(typeof(PersistentJob))
            .WithIdentity("Job", "Group")
            .UsingJobData(nameof(PersistentJob.TestProperty), "Initial Value")
            .StoreDurably()
            .Build();

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger", "Group")
            .StartNow()
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Scheduler.ScheduleJob(job, triggerOne, CancellationToken.None);
        
        watcher.WaitForEvent(TimeSpan.FromMinutes(1));

        var checkJob = await Scheduler.GetJobDetail(new JobKey("Job", "Group"));

        checkJob.Should()
            .BeAssignableTo<IJobDetail>().Which
            .JobDataMap.Should().Contain
            (
                x => x.Key == nameof(PersistentJob.TestProperty)
                     &&
                     x.Value.Equals("Ok")
            );
    }

    [Fact(DisplayName = "If a schedule tries to replace a job Then no concurrency exception is thrown")]
    public async Task If_a_schedule_tries_to_replace_a_job_Then_no_concurrency_exception_is_thrown()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();

        var job = JobBuilder
            .Create(typeof(PersistentJob))
            .WithIdentity("Job", "Group")
            .UsingJobData(nameof(PersistentJob.TestProperty), "Initial Value")
            .StoreDurably()
            .Build();

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger", "Group")
            .StartAt(DateTimeOffset.UtcNow.AddDays(1))
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Scheduler.ScheduleJob(job, triggerOne, CancellationToken.None);

        await Scheduler.Shutdown();
        
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();

        await Scheduler.Invoking(x => x.ScheduleJob(job, triggerOne, CancellationToken.None))
            .Should().NotThrowAsync<ConcurrencyException>();
    }

    [Fact(DisplayName = "If a schedule tries to replace a job with replace Then no exception is thrown")]
    public async Task If_a_schedule_tries_to_replace_a_job_with_replace_Then_no_exception_is_thrown()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();

        var job = JobBuilder
            .Create(typeof(PersistentJob))
            .WithIdentity("Job", "Group")
            .UsingJobData(nameof(PersistentJob.TestProperty), "Initial Value")
            .StoreDurably()
            .Build();

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger", "Group")
            .StartAt(DateTimeOffset.UtcNow.AddDays(1))
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Scheduler.ScheduleJob(job, triggerOne, CancellationToken.None);

        await Scheduler.Shutdown();
        
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
        await Scheduler.Start();

        await Scheduler.Invoking(x => x.ScheduleJob(job, new [] { triggerOne }, true, CancellationToken.None))
            .Should().NotThrowAsync();
    }

    [Fact(DisplayName = "If jobs are scheduled to a not started scheduler Then it does not throw")]
    public async Task If_jobs_are_scheduled_to_a_not_started_scheduler_Then_it_does_not_throw()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");

        var job = JobBuilder
            .Create(typeof(PersistentJob))
            .WithIdentity("Job", "Group")
            .UsingJobData(nameof(PersistentJob.TestProperty), "Initial Value")
            .StoreDurably()
            .Build();

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger", "Group")
            .StartAt(DateTimeOffset.UtcNow.AddDays(1))
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Scheduler.Invoking(x => x.ScheduleJob(job, triggerOne, CancellationToken.None))
            .Should().NotThrowAsync();
    }

    [Fact(DisplayName = "If a scheduler is not started Then it can be queried anyway")]
    public async Task If_a_scheduler_is_not_started_Then_it_can_be_queried_anyway()
    {
        Scheduler = await CreateSingleSchedulerAsync("Test", collectionName: "SchedulerData");
    
        await Scheduler.Invoking
            (
                x => x.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("Group"), CancellationToken.None)
            )
            .Should().NotThrowAsync();
    }
}
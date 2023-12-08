using FluentAssertions;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;
using Raven.Client.Documents;
using Xunit.Abstractions;

namespace Quartz.Impl.UnitTests;

using RavenJobStore;

public class ImplementationTests : TestBase
{
    private RavenJobStore Target { get; }
    
    private ITestOutputHelper Output { get; }

    public ImplementationTests(ITestOutputHelper output)
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
        var checkJob = await session.LoadAsync<Job>(job.Key.GetDatabaseId(Target.InstanceName));
        var checkTrigger = await session.LoadAsync<Trigger>(trigger.Key.GetDatabaseId(Target.InstanceName));

        checkJob.Should().NotBeNull();
        checkTrigger.Should().NotBeNull();
    }

    [Fact(DisplayName = "If StoreJobAndTrigger is called on and the elements exist Then they are replaced")]
    public async Task If_StoreJobAndTrigger_is_called_on_and_the_elements_exist_Then_they_are_replaced()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var jobOne = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "One"
        };
        var triggerOne = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = jobOne.Name,
            JobGroup = jobOne.Group,
            Description = "One"
        };
        await Target.StoreJobAndTriggerAsync(jobOne, triggerOne, CancellationToken.None);
        
        var jobTwo = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "Two"
        };
        var triggerTwo = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = jobOne.Name,
            JobGroup = jobOne.Group,
            Description = "Two"
        };
        await Target.StoreJobAndTriggerAsync(jobTwo, triggerTwo, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();

        var jobCount = await session.Query<Job>().CountAsync();
        var triggerCount = await session.Query<Trigger>().CountAsync();

        jobCount.Should().Be(1);
        triggerCount.Should().Be(1);
        
        var checkJob = await session.LoadAsync<Job>(jobOne.Key.GetDatabaseId(Target.InstanceName));
        var checkTrigger = await session.LoadAsync<Trigger>(triggerOne.Key.GetDatabaseId(Target.InstanceName));

        checkJob.Should()
            .NotBeNull().And
            .BeOfType<Job>().Which
            .Description.Should().Be("Two");
        checkTrigger.Should()
            .NotBeNull().And
            .BeOfType<Trigger>().Which
            .Description.Should().Be("Two");
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
 
        await Target.PauseJobsAsync(GroupMatcher<JobKey>.GroupEquals("known"), CancellationToken.None);

        var result = await Target.IsJobGroupPausedAsync("known", CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If no scheduler exists Then IsJobGroupPaused does not throw")]
    public async Task If_no_scheduler_exists_Then_IsJobGroupPaused_does_not_throw()
    {
        var call = () => Target.IsJobGroupPausedAsync("unknown", CancellationToken.None);
        await call.Should().NotThrowAsync();
    }

    [Fact(DisplayName = "If requested group is unknown Then IsTriggerGroupPaused returns false")]
    public async Task If_requested_group_is_unknown_Then_IsTriggerGroupPaused_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        var result = await Target.IsTriggerGroupPausedAsync("unknown", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If requested group is known and paused Then IsTriggerGroupPaused returns true")]
    public async Task If_requested_group_is_known_and_paused_Then_IsTriggerGroupPaused_returns_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
 
        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var trigger = new SimpleTriggerImpl("Trigger", "known")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        var set = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { job, new[] { trigger } }
        };

        await Target.StoreJobsAndTriggersAsync(set, false, CancellationToken.None);
        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("known"), CancellationToken.None);

        var result = await Target.IsTriggerGroupPausedAsync("known", CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If requested group is known and paused Then matching triggers are paused")]
    public async Task If_requested_group_is_known_and_paused_Then_matching_triggers_are_paused()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
 
        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var trigger = new SimpleTriggerImpl("Trigger", "known")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        var set = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { job, new[] { trigger } }
        };

        await Target.StoreJobsAndTriggersAsync(set, false, CancellationToken.None);
        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("known"), CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "known"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Paused);
    }

    [Fact(DisplayName = "If no scheduler exists Then IsTriggerGroupPaused does not throw")]
    public async Task If_no_scheduler_exists_Then_IsTriggerGroupPaused_does_not_throw()
    {
        var call = () => Target.IsTriggerGroupPausedAsync("unknown", CancellationToken.None);
        await call.Should().NotThrowAsync();
    }

    [Fact(DisplayName = "If a job does not exist Then StoreJob persists a new one")]
    public async Task If_a_job_does_not_exist_Then_StoreJob_persists_a_new_one()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJob = await session.LoadAsync<Job>(job.Key.GetDatabaseId(Target.InstanceName));

        checkJob.Should().NotBeNull();
    }

    [Fact(DisplayName = "If a job exists and replace is false Then StoreJob throws")]
    public async Task If_a_job_exists_and_replace_is_false_Then_StoreJob_throws()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var jobOne = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var jobTwo = new JobDetailImpl("Job", "Group", typeof(NoOpJob));

        await Target.StoreJobAsync(jobOne, false, CancellationToken.None);

        var call = () => Target.StoreJobAsync(jobTwo, false, CancellationToken.None);

        await call.Should().ThrowAsync<ObjectAlreadyExistsException>();
    }

    [Fact(DisplayName = "If a job exists and replace is true Then StoreJob overwrites the existing one")]
    public async Task If_a_job_exists_and_replace_is_true_Then_StoreJob_overwrites_the_existing_one()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var jobOne = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "One"
        };
        var jobTwo = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "Two"
        };

        await Target.StoreJobAsync(jobOne, false, CancellationToken.None);
        await Target.StoreJobAsync(jobTwo, true, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();
        
        checkJobs.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Two");
    }

    [Fact(DisplayName = "If StoreJobsAndTriggers is called on a valid store Then job and trigger are created")]
    public async Task If_StoreJobsAndTriggers_is_called_on_a_valid_store_Then_job_and_trigger_are_created()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        var set = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { job, new[] { trigger } }
        };

        await Target.StoreJobsAndTriggersAsync(set, false, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJob = await session.LoadAsync<Job>(job.Key.GetDatabaseId(Target.InstanceName));
        var checkTrigger = await session.LoadAsync<Trigger>(trigger.Key.GetDatabaseId(Target.InstanceName));

        checkJob.Should().NotBeNull();
        checkTrigger.Should().NotBeNull();
    }

    [Fact(DisplayName = "If StoreJobsAndTriggers is called with replace false and job exists Then it throws")]
    public async Task If_StoreJobsAndTriggers_is_called_with_replace_false_and_job_exists_Then_it_throws()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var jobOne = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "One"
        };
        await Target.StoreJobAsync(jobOne, false, CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        var set = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { job, new[] { trigger } }
        };

        var call = () =>Target.StoreJobsAndTriggersAsync(set, false, CancellationToken.None);

        await call.Should().ThrowAsync<ObjectAlreadyExistsException>();
    }

    [Fact(DisplayName = "If StoreJobsAndTriggers is called with replace false and trigger exists Then it throws")]
    public async Task If_StoreJobsAndTriggers_is_called_with_replace_false_and_trigger_exists_Then_it_throws()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        await session.StoreAsync
        (
            new Trigger(new SimpleTriggerImpl("Trigger", "Group"), Target.InstanceName)
            {
                State = InternalTriggerState.Paused
            }
        );

        await session.SaveChangesAsync(); 

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };

        var set = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { job, new[] { trigger } }
        };

        var call = () =>Target.StoreJobsAndTriggersAsync(set, false, CancellationToken.None);

        await call.Should().ThrowAsync<ObjectAlreadyExistsException>();
    }

    [Fact(DisplayName = "If StoreJobsAndTriggers is called with replace true Then existing elements are replaced")]
    public async Task If_StoreJobsAndTriggers_is_called_with_replace_true_Then_existing_elements_are_replaced()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var jobOne = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "One"
        };
        var triggerOne = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = jobOne.Name,
            JobGroup = jobOne.Group,
            Description = "One"
        };
        var setOne = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { jobOne, new[] { triggerOne } }
        };

        var jobTwo = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Description = "Two"
        };
        var triggerTwo = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = jobTwo.Name,
            JobGroup = jobTwo.Group,
            Description = "Two"
        };
        var setTwo = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
        {
            { jobTwo, new[] { triggerTwo } }
        };

        await Target.StoreJobsAndTriggersAsync(setOne, false, CancellationToken.None);
        await Target.StoreJobsAndTriggersAsync(setTwo, true, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        checkJobs.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Two");
        checkTriggers.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Two");
    }

    [Fact(DisplayName = "If a job exists Then RemoveJob will remove it and return true")]
    public async Task If_a_job_exists_Then_RemoveJob_will_remove_it_and_return_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var result = await Target.RemoveJobAsync(job.Key, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        checkJobs.Should().BeEmpty();
        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a job does not exist Then RemoveJob will do nothing and return false")]
    public async Task If_a_job_does_not_exist_Then_RemoveJob_will_do_nothing_and_return_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RemoveJobAsync(new JobKey("Job", "Group"), CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        checkJobs.Should().BeEmpty();
        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a job exists Then RemoveJobs will remove it and return true")]
    public async Task If_a_job_exists_Then_RemoveJobs_will_remove_it_and_return_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var result = await Target.RemoveJobsAsync(new[] { job.Key }, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        checkJobs.Should().BeEmpty();
        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a job does not exist Then RemoveJobs will do nothing and return true")]
    public async Task If_a_job_does_not_exist_Then_RemoveJobs_will_do_nothing_and_return_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RemoveJobsAsync
        (
            new[] { new JobKey("Job", "Group") },
            CancellationToken.None
        );

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        checkJobs.Should().BeEmpty();
        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a job exists The RetrieveJob will return its data")]
    public async Task If_a_job_exists_The_RetrieveJob_will_return_its_data()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var data = await Target.RetrieveJobAsync
        (
            new JobKey("Job", "Group"),
            CancellationToken.None
        );

        data.Should()
            .BeAssignableTo<IJobDetail>().Which
            .Key.Name.Should().Be("Job");
    }

    [Fact(DisplayName = "If a job does not exist The RetrieveJob will return null")]
    public async Task If_a_job_does_not_exist_The_RetrieveJob_will_return_null()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var data = await Target.RetrieveJobAsync
        (
            new JobKey("Job", "Group"),
            CancellationToken.None
        );

        data.Should().BeNull();
    }

    [Fact(DisplayName = "If a trigger does not exist and the referenced job exists Then StoreTrigger will succeed")]
    public async Task If_a_trigger_does_not_exist_and_the_referenced_job_exists_Then_StoreTrigger_will_succeed()
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
        
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        checkTriggers.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.TriggerKey.Equals(new TriggerKey("Trigger", "Group")));
    }

    [Fact(DisplayName = "If a trigger and the referenced job does not exist Then StoreTrigger will throw")]
    public async Task If_a_trigger_and_the_referenced_job_does_not_exist_Then_StoreTrigger_will_throw()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = "Job",
            JobGroup = "Group"
        };

        var call = () => Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        await call.Should().ThrowAsync<JobPersistenceException>();
    }

    [Fact(DisplayName = "If a trigger exists and the referenced job exists Then StoreTrigger without replace will throw")]
    public async Task If_a_trigger_exists_and_the_referenced_job_exists_Then_StoreTrigger_without_replace_will_throw()
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
        var call = () => Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        await call.Should().ThrowAsync<ObjectAlreadyExistsException>();
    }

    [Fact(DisplayName = "If a trigger exists and the referenced job exists Then StoreTrigger with replace will succeed")]
    public async Task If_a_trigger_exists_and_the_referenced_job_exists_Then_StoreTrigger_with_replace_will_succeed()
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

        trigger.Description = "Replace";
        
        await Target.StoreTriggerAsync(trigger, true, CancellationToken.None);
        
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        checkTriggers.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Replace");
    }

    [Fact(DisplayName = "If a trigger does not exist Then RemoveTrigger will return false")]
    public async Task If_a_trigger_does_not_exist_Then_RemoveTrigger_will_return_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RemoveTriggerAsync
        (
            new TriggerKey("unknown", "unknown"),
            CancellationToken.None
        );

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a trigger exists Then RemoveTrigger will delete it")]
    public async Task If_a_trigger_exists_Then_RemoveTrigger_will_delete_it()
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

        var result = await Target.RemoveTriggerAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        result.Should().BeTrue();

        checkTriggers.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If the associated job is not durable Then RemoveTrigger will delete it as well")]
    public async Task If_the_associated_job_is_not_durable_Then_RemoveTrigger_will_delete_it_as_well()
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

        var result = await Target.RemoveTriggerAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If the associated job is durable Then RemoveTrigger will not delete it")]
    public async Task If_the_associated_job_is_durable_Then_RemoveTrigger_will_not_delete_it()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Durable = true,
            Description = "Durable"
        };
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var result = await Target.RemoveTriggerAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Durable");
    }

    [Fact(DisplayName = "If more then one trigger for a job exist Then RemoveTrigger will not delete the job")]
    public async Task If_more_then_one_trigger_for_a_job_exist_Then_RemoveTrigger_will_not_delete_the_job()
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
        
        trigger = new SimpleTriggerImpl("TriggerTwo", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var result = await Target.RemoveTriggerAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Name == "Job");
    }

    [Fact(DisplayName = "If a trigger does not exist Then RemoveTriggers will return false")]
    public async Task If_a_trigger_does_not_exist_Then_RemoveTriggers_will_return_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RemoveTriggersAsync
        (
            new[] {new TriggerKey("unknown", "unknown") },
            CancellationToken.None
        );

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a trigger exists Then RemoveTriggers will delete it")]
    public async Task If_a_trigger_exists_Then_RemoveTriggers_will_delete_it()
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

        var result = await Target.RemoveTriggersAsync
        (
            new[] { new TriggerKey("Trigger", "Group") },
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        result.Should().BeTrue();

        checkTriggers.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If the associated job is not durable Then RemoveTriggers will delete it as well")]
    public async Task If_the_associated_job_is_not_durable_Then_RemoveTriggers_will_delete_it_as_well()
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

        var result = await Target.RemoveTriggersAsync
        (
            new[] { new TriggerKey("Trigger", "Group") },
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If the associated job is durable Then RemoveTriggers will not delete it")]
    public async Task If_the_associated_job_is_durable_Then_RemoveTriggers_will_not_delete_it()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob))
        {
            Durable = true,
            Description = "Durable"
        };
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var trigger = new SimpleTriggerImpl("Trigger", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var result = await Target.RemoveTriggersAsync
        (
            new[] { new TriggerKey("Trigger", "Group") },
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Durable");
    }

    [Fact(DisplayName = "If more then one trigger for a job exist Then RemoveTriggers will not delete the job")]
    public async Task If_more_then_one_trigger_for_a_job_exist_Then_RemoveTriggers_will_not_delete_the_job()
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
        
        trigger = new SimpleTriggerImpl("TriggerTwo", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var result = await Target.RemoveTriggersAsync
        (
            new[] { new TriggerKey("Trigger", "Group") },
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Name == "Job");
    }

    [Fact(DisplayName = "If all triggers for a job are removed Then RemoveTriggers will delete the job")]
    public async Task If_all_triggers_for_a_job_are_removed_Then_RemoveTriggers_will_delete_the_job()
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
        
        trigger = new SimpleTriggerImpl("TriggerTwo", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var result = await Target.RemoveTriggersAsync
        (
            new[] { new TriggerKey("Trigger", "Group"), new TriggerKey("TriggerTwo", "Group") },
            CancellationToken.None
        );
         
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        result.Should().BeTrue();

        checkJobs.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If a trigger does not exist Then ReplaceTrigger will return false and not store the new one")]
    public async Task If_a_trigger_does_not_exist_Then_ReplaceTrigger_will_return_false_and_not_store_the_new_one()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var trigger = new SimpleTriggerImpl("Trigger", "Group");

        var result = await Target.ReplaceTriggerAsync(trigger.Key, trigger, CancellationToken.None);
        
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        result.Should().BeFalse();

        checkTriggers.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If a trigger exists but the referenced job not Then ReplaceTrigger will throw")]
    public async Task If_a_trigger_exists_but_the_referenced_job_not_Then_ReplaceTrigger_will_throw()
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

        trigger.JobName = "Unknown";
        trigger.JobGroup = "Unknown";
        
        var call = () => Target.ReplaceTriggerAsync(trigger.Key, trigger, CancellationToken.None);

        await call.Should().ThrowExactlyAsync<JobPersistenceException>();
    }

    [Fact(DisplayName = "If a trigger and job exist Then ReplaceTrigger will replace the trigger")]
    public async Task If_a_trigger_and_job_exist_Then_ReplaceTrigger_will_replace_the_trigger()
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

        trigger.Description = "Replacement";
        
        var result = await Target.ReplaceTriggerAsync(trigger.Key, trigger, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        result.Should().BeTrue();

        checkTriggers.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Replacement");
    }

    [Fact(DisplayName = "If a trigger exists Then RetrieveTrigger returns it")]
    public async Task If_a_trigger_exists_Then_RetrieveTrigger_returns_it()
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

        var result = await Target.RetrieveTriggerAsync(trigger.Key, CancellationToken.None);

        result.Should().NotBeNull();
    }

    [Fact(DisplayName = "If a trigger exists Then RetrieveTrigger returns it in a certain state")]
    public async Task If_a_trigger_exists_Then_RetrieveTrigger_returns_it_in_a_certain_state()
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

        var result = await Target.RetrieveTriggerAsync(trigger.Key, CancellationToken.None);
        result.ThrowIfNull();

        result.Should().BeOfType<SimpleTriggerImpl>();
        result.JobKey.Should().Be(trigger.JobKey);
        result.Key.Should().Be(trigger.Key);
    }

    [Fact(DisplayName = "If a trigger does not exist Then RetrieveTrigger returns it")]
    public async Task If_a_trigger_does_not_exist_Then_RetrieveTrigger_returns_it()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RetrieveTriggerAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        result.Should().BeNull();
    }

    [Fact(DisplayName = "If a calendar exists Then CalendarExists returns true")]
    public async Task If_a_calendar_exists_Then_CalendarExists_returns_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        await Target.StoreCalendar("test", new BaseCalendar(), true, true);

        var result = await Target.CalendarExistsAsync("test", CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a calendar does not exist Then CalendarExists returns false")]
    public async Task If_a_calendar_does_not_exist_Then_CalendarExists_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.CalendarExistsAsync("test", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a job does not exist Then CheckExists returns false")]
    public async Task If_a_job_does_not_exist_Then_CheckExists_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.CheckExistsAsync(new JobKey("name", "group"), CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a job exists Then CheckExists returns true")]
    public async Task If_a_job_exists_Then_CheckExists_returns_true()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var result = await Target.CheckExistsAsync(new JobKey("Job", "Group"), CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a trigger does not exist Then CheckExists returns false")]
    public async Task If_a_trigger_does_not_exist_Then_CheckExists_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.CheckExistsAsync(new TriggerKey("Trigger", "group"), CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a trigger exists Then CheckExists returns true")]
    public async Task If_a_trigger_exists_Then_CheckExists_returns_true()
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

        var result = await Target.CheckExistsAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a trigger exists Then ClearAllSchedulingData removes it")]
    public async Task If_a_trigger_exists_Then_ClearAllSchedulingData_removes_it()
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

        await Target.ClearAllSchedulingDataAsync(CancellationToken.None);
    
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkTriggers = await session.Query<Trigger>().ToListAsync();

        checkTriggers.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If a job exists Then ClearAllSchedulingData removes it")]
    public async Task If_a_job_exists_Then_ClearAllSchedulingData_removes_it()
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

        await Target.ClearAllSchedulingDataAsync(CancellationToken.None);
    
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJobs = await session.Query<Job>().ToListAsync();

        checkJobs.Should().HaveCount(0);
    }

    [Fact(DisplayName = "If a blocked jobs exists Then ClearAllSchedulingData removes it")]
    public async Task If_a_blocked_job_exists_Then_ClearAllSchedulingData_removes_it()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        using (var arrangeSession = Target.DocumentStore!.OpenAsyncSession())
        {
            await arrangeSession.StoreAsync(new BlockedJob(Target.InstanceName, "X"));
            await arrangeSession.SaveChangesAsync();
        }

        await Target.ClearAllSchedulingDataAsync(CancellationToken.None);
    
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var anyBlocks = await Target.GetBlockedJobsAsync(session, CancellationToken.None);

        anyBlocks.Should().BeEmpty();
    }

    [Fact(DisplayName = "If a calendar exist Then ClearAllSchedulingData removes it")]
    public async Task If_a_calendar_exist_Then_ClearAllSchedulingData_removes_it()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        await Target.StoreCalendarAsync("test", new BaseCalendar(), true, true, CancellationToken.None);

        await Target.ClearAllSchedulingDataAsync(CancellationToken.None);

        var exists = await Target.CalendarExistsAsync("Test", CancellationToken.None);

        exists.Should().BeFalse();
    }

    [Fact(DisplayName = "If a calendar exists and StoreCalendar tries to replace w/o flag Then it throws")]
    public async Task If_a_calendar_exists_and_StoreCalendar_tries_to_replace_wo_flag_Then_it_throws()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
        await Target.StoreCalendarAsync("test", new BaseCalendar(), true, true, CancellationToken.None);
        
        var call = () => Target.StoreCalendar("test", new BaseCalendar(), false, true);
        
        await call.Should().ThrowAsync<ObjectAlreadyExistsException>();
    }

    [Fact(DisplayName = "If StoreCalendar updates triggers Then triggers are changed accordingly")]
    public async Task If_StoreCalendar_updates_triggers_Then_triggers_are_changed_accordingly()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var trigger = new SimpleTriggerImpl
        (
            "Trigger",
            "Group",
            job.Name,
            job.Group,
            SystemTime.UtcNow().AddMinutes(2),
            null,
            SimpleTriggerImpl.RepeatIndefinitely,
            TimeSpan.FromMinutes(1)
        )
        {
            CalendarName = "test"
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var calendarStart = trigger.StartTimeUtc.AddMinutes(30);

        var calendar = new DailyCalendar
        (
            calendarStart.UtcDateTime,
            calendarStart.AddMinutes(30).UtcDateTime
        ) { InvertTimeRange = true, TimeZone = TimeZoneInfo.Utc };

        var nextFireTime = trigger.GetFireTimeAfter(null)!.Value;
        var isIncluded = calendar.IsTimeIncluded(nextFireTime);

        isIncluded.Should().BeFalse
        (
            "Calendar range {0} - {1} should not include {2}",
            calendar.RangeStartingTime,
            calendar.RangeEndingTime,
            nextFireTime
        );
        
        await Target.StoreCalendarAsync("test", calendar, true, true, CancellationToken.None);

        var checkTrigger = await Target.RetrieveTriggerAsync(trigger.Key, CancellationToken.None);
        checkTrigger!.GetNextFireTimeUtc().Should().Be(calendarStart);
    }

    [Fact(DisplayName = "If RemoveCalendar names an existing entry Then the calendar is removed and true returned")]
    public async Task If_RemoveCalendar_names_an_existing_entry_Then_the_calendar_is_removed_and_true_returned()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var calendarStart = DateTimeOffset.UtcNow;

        var calendar = new DailyCalendar(calendarStart.Ticks, calendarStart.AddHours(1).Ticks);
        await Target.StoreCalendarAsync("test", calendar, true, true, CancellationToken.None);

        var result = await Target.RemoveCalendarAsync("test", CancellationToken.None);

        result.Should().BeTrue();
        (await Target.CalendarExistsAsync("test", CancellationToken.None)).Should().BeFalse();
    }

    [Fact(DisplayName = "If entry does not exist Then the RemoveCalendar returns false")]
    public async Task If_entry_does_not_exist_Then_the_RemoveCalendar_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RemoveCalendarAsync("test", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If RemoveCalendar returns false Then other calendars are not affected")]
    public async Task If_RemoveCalendar_returns_false_Then_other_calendars_are_not_affected()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var calendarStart = DateTimeOffset.UtcNow;

        var calendar = new DailyCalendar(calendarStart.Ticks, calendarStart.AddHours(1).Ticks);
        await Target.StoreCalendarAsync("test", calendar, true, true, CancellationToken.None);

        var result = await Target.RemoveCalendarAsync("test1", CancellationToken.None);

        result.Should().BeFalse();
        (await Target.CalendarExistsAsync("test", CancellationToken.None)).Should().BeTrue();
    }

    [Fact(DisplayName = "If a calendar exists Then RetrieveCalendar returns it")]
    public async Task If_a_calendar_exists_Then_RetrieveCalendar_returns_it()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var calendarStart = DateTimeOffset.UtcNow;

        var calendar = new DailyCalendar(calendarStart.Ticks, calendarStart.AddHours(1).Ticks);
        await Target.StoreCalendarAsync("test", calendar, true, true, CancellationToken.None);

        var result = await Target.RetrieveCalendarAsync("test", CancellationToken.None);

        result.Should().BeOfType<DailyCalendar>();
    }

    [Fact(DisplayName = "If a calendar does not exist Then RetrieveCalendar returns null")]
    public async Task If_a_calendar_does_not_exist_Then_RetrieveCalendar_returns_null()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var result = await Target.RetrieveCalendarAsync("test", CancellationToken.None);

        result.Should().BeNull();
    }

    [Theory(DisplayName = "If GetNumberOfJobs is called Then it returns the number of existing jobs")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task If_GetNumberOfJobs_is_called_Then_it_returns_the_number_of_existing_jobs(int count)
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        for (var index = 1; index <= count; ++index)
        {
            var job = new JobDetailImpl($"Job{index}", "Group", typeof(NoOpJob));
            await Target.StoreJobAsync(job, false, CancellationToken.None);
        }

        var result = await Target.GetNumberOfJobsAsync(CancellationToken.None);

        result.Should().Be(count);
    }

    [Theory(DisplayName = "If GetNumberOfTriggers is called Then it returns the number of existing triggers")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task If_GetNumberOfTriggers_is_called_Then_it_returns_the_number_of_existing_triggers(int count)
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        for (var index = 1; index <= count; ++index)
        {
            var trigger = new SimpleTriggerImpl($"Trigger{index}", "Group")
            {
                JobName = job.Name,
                JobGroup = job.Group
            };

            await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);
        }

        var result = await Target.GetNumberOfTriggersAsync(CancellationToken.None);

        result.Should().Be(count);
    }

    [Theory(DisplayName = "If GetNumberOfCalendars is called Then it returns the number of existing calendars")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task If_GetNumberOfCalendars_is_called_Then_it_returns_the_number_of_existing_calendars(int count)
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        for (var index = 1; index <= count; ++index)
        {
            await Target.StoreCalendarAsync
            (
                $"test{index}",
                new BaseCalendar(),
                true,
                true,
                CancellationToken.None
            );
        }

        var result = await Target.GetNumberOfCalendarsAsync(CancellationToken.None);

        result.Should().Be(count);
    }

    [Fact(DisplayName = "If a group matcher equals is used Then GetJobKeys finds entries")]
    public async Task If_a_group_matcher_equals_is_used_Then_GetJobKeys_finds_entries()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group1", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job", "Group2", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job", "Group3", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        
        WaitForIndexing(Target.DocumentStore);

        var result = await Target.GetJobKeysAsync
            
        (
            GroupMatcher<JobKey>.GroupEquals("Group2"), CancellationToken.None
        );

        result.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Group == "Group2" && x.Name == "Job");
    }

    [Fact(DisplayName = "If a group matcher ends with is used Then GetJobKeys finds entries")]
    public async Task If_a_group_matcher_ends_with_is_used_Then_GetJobKeys_finds_entries()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group1", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job", "Group2", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job", "Group3", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var result = await Target.GetJobKeysAsync
        (
            GroupMatcher<JobKey>.GroupEndsWith("2"), CancellationToken.None
        );

        result.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Group == "Group2" && x.Name == "Job");
    }

    [Fact(DisplayName = "If a group matcher starts with is used Then GetJobKeys finds entries")]
    public async Task If_a_group_matcher_starts_with_is_used_Then_GetJobKeys_finds_entries()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "1Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job", "2Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job", "3Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var result = await Target.GetJobKeysAsync
        (
            GroupMatcher<JobKey>.GroupStartsWith("2"), CancellationToken.None
        );

        result.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Group == "2Group" && x.Name == "Job");
    }

    [Fact(DisplayName = "If a group matcher equals is used Then GetTriggerKeys finds entries")]
    public async Task If_a_group_matcher_equals_is_used_Then_GetTriggerKeys_finds_entries()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group1", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group1") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        job = new JobDetailImpl("Job", "Group2", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group2") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        job = new JobDetailImpl("Job", "Group3", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group3") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        var result = await Target.GetTriggerKeysAsync
        (
            GroupMatcher<TriggerKey>.GroupEquals("Group2"), CancellationToken.None
        );

        result.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Group == "Group2" && x.Name == "Trigger");
    }

    [Fact(DisplayName = "If two job groups exist Then GetJobGroupNames lists them")]
    public async Task If_two_job_groups_exist_Then_GetJobGroupNames_lists_them()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job1", "Group1", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job2", "Group1", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job3", "Group2", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        job = new JobDetailImpl("Job4", "Group2", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var result = await Target.GetJobGroupNamesAsync(CancellationToken.None);

        result.Should()
            .HaveCount(2).And
            .ContainSingle(x => x == "Group1").And
            .ContainSingle(x => x == "Group2");
    }

    [Fact(DisplayName = "If two trigger groups exist Then GetTriggerGroupNames lists them")]
    public async Task If_two_trigger_groups_exist_Then_GetTriggerGroupNames_lists_them()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group1", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger1", "Group1") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        job = new JobDetailImpl("Job", "Group2", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger2", "Group1") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        job = new JobDetailImpl("Job", "Group3", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger3", "Group2") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        job = new JobDetailImpl("Job", "Group4", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger4", "Group2") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        var result = await Target.GetTriggerGroupNamesAsync(CancellationToken.None);
        
        result.Should()
            .HaveCount(2).And
            .ContainSingle(x => x == "Group1").And
            .ContainSingle(x => x == "Group2");
    }

    [Fact(DisplayName = "If some calendars exist Then GetCalendarNames fetches their names")]
    public async Task If_some_calendars_exist_Then_GetCalendarNames_fetches_their_names()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        await Target.StoreCalendar("test1", new BaseCalendar(), true, true);
        await Target.StoreCalendar("test2", new BaseCalendar(), true, true);

        var result = await Target.GetCalendarNamesAsync(CancellationToken.None);

        result.Should()
            .HaveCount(2).And
            .ContainSingle(x => x == "test1").And
            .ContainSingle(x => x == "test2");
    }

    [Fact(DisplayName = "If multiple triggers reference a job Then GetTriggersForJob fetches them")]
    public async Task If_multiple_triggers_reference_a_job_Then_GetTriggersForJob_fetches_them()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger1", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger2", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        var result = await Target.GetTriggersForJobAsync(job.Key, CancellationToken.None);

        result.Should()
            .HaveCount(2).And
            .ContainSingle(x => x.Key.Name == "Trigger1").And
            .ContainSingle(x => x.Key.Name == "Trigger2");
    }

    [Theory(DisplayName = "If a trigger has a specific internal state Then GetTriggerState yields the expected state")]
    [InlineData(InternalTriggerState.Paused, TriggerState.Paused)]
    [InlineData(InternalTriggerState.Complete, TriggerState.Complete)]
    [InlineData(InternalTriggerState.Blocked, TriggerState.Blocked)]
    [InlineData(InternalTriggerState.PausedAndBlocked, TriggerState.Paused)]
    [InlineData(InternalTriggerState.Error, TriggerState.Error)]
    [InlineData(InternalTriggerState.Acquired, TriggerState.Normal)]
    [InlineData(InternalTriggerState.Executing, TriggerState.Normal)]
    public async Task If_a_trigger_has_a_specific_internal_state_Then_GetTriggerState_yields_the_expected_state(
        InternalTriggerState given,
        TriggerState expected)
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State = given;

            await session.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        var result = await Target.GetTriggerStateAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        result.Should().Be(expected);
    }

    [Fact(DisplayName = "If a faulty trigger is blocked Then reset lets it enter the blocked state")]
    public async Task If_a_faulty_trigger_is_blocked_Then_reset_lets_it_enter_the_blocked_state()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State = InternalTriggerState.Error;

            await session.StoreAsync
            (
                new BlockedJob(Target.InstanceName, job.Key.GetDatabaseId(Target.InstanceName))
            );

            await session.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        await Target.ResetTriggerFromErrorStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State.Should().Be(InternalTriggerState.Blocked);
        }
    }

    [Fact(DisplayName = "If a faulty trigger is paused Then reset lets it enter the paused state")]
    public async Task If_a_faulty_trigger_is_paused_Then_reset_lets_it_enter_the_paused_state()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State = InternalTriggerState.Error;

            await session.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);

        await Target.ResetTriggerFromErrorStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State.Should().Be(InternalTriggerState.Paused);
        }
    }

    [Fact(DisplayName = "If a faulty trigger is unblocked and resumed Then reset lets it enter the waiting state")]
    public async Task If_a_faulty_trigger_is_unblocked_and_resumed_Then_reset_lets_it_enter_the_waiting_state()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State = InternalTriggerState.Error;

            await session.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        await Target.ResetTriggerFromErrorStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State.Should().Be(InternalTriggerState.Waiting);
        }
    }

    [Fact(DisplayName = "If a trigger is paused Then its internal state is set accordingly")]
    public async Task If_a_trigger_is_paused_Then_its_internal_state_is_set_accordingly()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var trigger = await session.LoadAsync<Trigger>
        (
            Trigger.GetId(Target.InstanceName, "Group", "Trigger")
        );

        trigger.State.Should().Be(InternalTriggerState.Paused);
    }

    [Fact(DisplayName = "If a blocked trigger is paused Then its internal state is set accordingly")]
    public async Task If_a_blocked_trigger_is_paused_Then_its_internal_state_is_set_accordingly()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var arrangeSession = Target.DocumentStore!.OpenAsyncSession())
        {
            var entity = await arrangeSession.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, "Group", "Trigger")
            );
            entity.State = InternalTriggerState.Blocked;

            await arrangeSession.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        await Target.PauseTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var trigger = await session.LoadAsync<Trigger>
        (
            Trigger.GetId(Target.InstanceName, "Group", "Trigger")
        );

        trigger.State.Should().Be(InternalTriggerState.PausedAndBlocked);
    }

    [Fact(DisplayName = "If a trigger is paused Then the trigger group is not paused")]
    public async Task If_a_trigger_is_paused_Then_the_trigger_group_is_not_paused()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        var result = await Target.IsTriggerGroupPausedAsync("Group", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a trigger group is paused Then later added triggers are paused as well")]
    public async Task If_a_trigger_group_is_paused_Then_later_added_triggers_are_paused_as_well()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Paused);
    }

    [Fact(DisplayName = "If a job group is paused Then later added triggers are paused as well")]
    public async Task If_a_job_group_is_paused_Then_later_added_triggers_are_paused_as_well()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        await Target.PauseJobsAsync(GroupMatcher<JobKey>.GroupEquals("Group"), CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Paused);
    }

    [Fact(DisplayName = "If a specific job is paused Then all referencing triggers are paused")]
    public async Task If_a_specific_job_is_paused_Then_all_referencing_triggers_are_paused()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger1", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger2", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        
        await Target.PauseJobAsync(new JobKey("Job", "Group"), CancellationToken.None);

        var state1 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger1", "Group"),
            CancellationToken.None
        );
        var state2 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger2", "Group"),
            CancellationToken.None
        );
        
        state1.Should().Be(TriggerState.Paused);
        state2.Should().Be(TriggerState.Paused);
    }

    [Fact(DisplayName = "If a job group is paused Then existing matching triggers are paused")]
    public async Task If_a_job_group_is_paused_Then_existing_matching_triggers_are_paused()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger1", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger2", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        
        await Target.PauseJobsAsync(GroupMatcher<JobKey>.GroupEquals("Group"), CancellationToken.None);

        var state1 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger1", "Group"),
            CancellationToken.None
        );
        var state2 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger2", "Group"),
            CancellationToken.None
        );
        
        state1.Should().Be(TriggerState.Paused);
        state2.Should().Be(TriggerState.Paused);
    }

    [Fact(DisplayName = "If a paused not blocked trigger is resumed Then it goes to normal")]
    public async Task If_a_paused_not_blocked_trigger_is_resumed_Then_it_goes_to_normal()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);
        await Target.ResumeTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Normal);
    }

    [Fact(DisplayName = "If a paused and blocked trigger is resumed Then it goes to blocked")]
    public async Task If_a_paused_and_blocked_trigger_is_resumed_Then_it_goes_to_blocked()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            await session.StoreAsync
            (
                new BlockedJob(Target.InstanceName, job.Key.GetDatabaseId(Target.InstanceName))
            );

            await session.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        await Target.PauseTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);
        await Target.ResumeTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Blocked);
    }

    [Fact(DisplayName = "If a by group paused trigger is resumed Then it goes to normal")]
    public async Task If_a_by_group_paused_trigger_is_resumed_Then_it_goes_to_normal()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Normal);
    }

    [Fact(DisplayName = "If a by group paused trigger is resumed Then the group stays paused")]
    public async Task If_a_by_group_paused_trigger_is_resumed_Then_the_group_stays_paused()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeTriggerAsync(new TriggerKey("Trigger", "Group"), CancellationToken.None);

        var result = await Target.IsTriggerGroupPausedAsync("Group", CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact(DisplayName = "If a by group paused trigger is resumed by group Then it goes to normal")]
    public async Task If_a_by_group_paused_trigger_is_resumed_by_group_Then_it_goes_to_normal()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Normal);
    }

    [Fact(DisplayName = "If a by group paused trigger is resumed by group Then the group is resumed")]
    public async Task If_a_by_group_paused_trigger_is_resumed_by_group_Then_the_group_is_resumed()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);

        var result = await Target.IsTriggerGroupPausedAsync("Group", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a paused group is resumed Then the associated triggers are resumed")]
    public async Task If_a_paused_job_is_resumed_Then_the_associated_triggers_are_resumed()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger1", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger2", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        
        await Target.PauseJobAsync(new JobKey("Job", "Group"), CancellationToken.None);
        await Target.ResumeJobAsync(new JobKey("Job", "Group"), CancellationToken.None);

        var state1 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger1", "Group"),
            CancellationToken.None
        );
        var state2 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger2", "Group"),
            CancellationToken.None
        );
        
        state1.Should().Be(TriggerState.Normal);
        state2.Should().Be(TriggerState.Normal);
    }

    [Fact(DisplayName = "If a paused job is resumed Then a previously blocked trigger is blocked again")]
    public async Task If_a_paused_job_is_resumed_Then_a_previously_blocked_trigger_is_blocked_again()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        using (var session = Target.DocumentStore!.OpenAsyncSession())
        {
            await session.StoreAsync
            (
                new BlockedJob(Target.InstanceName, Job.GetId(Target.InstanceName, "Group", "Job"))
            );
            
            await session.SaveChangesAsync();
        }
        
        WaitForIndexing(Target.DocumentStore!);

        await Target.PauseJobAsync(new JobKey("Job", "Group"), CancellationToken.None);
        await Target.ResumeJobAsync(new JobKey("Job", "Group"), CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Blocked);
    }

    [Fact(DisplayName = "If a by group paused trigger is resumed by resume all Then it goes to normal")]
    public async Task If_a_by_group_paused_trigger_is_resumed_by_resume_all_Then_it_goes_to_normal()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeAllTriggersAsync(CancellationToken.None);

        var state = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger", "Group"),
            CancellationToken.None
        );

        state.Should().Be(TriggerState.Normal);
    }

    [Fact(DisplayName = "If a by group paused trigger is resumed by resume all Then the group is resumed")]
    public async Task If_a_by_group_paused_trigger_is_resumed_by_resume_all_Then_the_group_is_resumed()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );

        await Target.PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeAllTriggersAsync(CancellationToken.None);

        var result = await Target.IsTriggerGroupPausedAsync("Group", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If job group is resumed by group Then IsJobGroupPaused returns false")]
    public async Task If_job_group_is_resumed_by_group_Then_IsJobGroupPaused_returns_false()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);
 
        await Target.PauseJobsAsync(GroupMatcher<JobKey>.GroupEquals("known"), CancellationToken.None);
        await Target.ResumeJobsAsync(GroupMatcher<JobKey>.GroupEquals("known"), CancellationToken.None);

        var result = await Target.IsJobGroupPausedAsync("known", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact(DisplayName = "If a paused job group is resumed by group Then the associated triggers are resumed")]
    public async Task If_a_paused_job_group_is_resumed_by_group_Then_the_associated_triggers_are_resumed()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger1", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        await Target.StoreTriggerAsync
        (
            new SimpleTriggerImpl("Trigger2", "Group") { JobName = job.Name, JobGroup = job.Group },
            false,
            CancellationToken.None
        );
        
        await Target.PauseJobsAsync(GroupMatcher<JobKey>.GroupEquals("Group"), CancellationToken.None);
        await Target.ResumeJobsAsync(GroupMatcher<JobKey>.GroupEquals("Group"), CancellationToken.None);

        var state1 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger1", "Group"),
            CancellationToken.None
        );
        var state2 = await Target.GetTriggerStateAsync
        (
            new TriggerKey("Trigger2", "Group"),
            CancellationToken.None
        );
        
        state1.Should().Be(TriggerState.Normal);
        state2.Should().Be(TriggerState.Normal);
    }

    [Fact(DisplayName = "If a trigger matches the given slot Then it will be acquired")]
    public async Task If_a_trigger_matches_the_given_slot_Then_it_will_be_acquired()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger1", "Group")
            .StartNow()
            .WithDescription("Expected")
            .ForJob(job)
            .Build();
        var triggerTwo = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger2", "Group")
            .StartAt(DateTimeOffset.UtcNow.AddDays(1))
            .WithDescription("Unexpected")
            .ForJob(job)
            .Build();

        triggerOne.ComputeFirstFireTimeUtc(null);
        triggerTwo.ComputeFirstFireTimeUtc(null);
        
        await Target.StoreTriggerAsync
        (
            triggerOne,
            false,
            CancellationToken.None
        );
        await Target.StoreTriggerAsync
        (
            triggerTwo,
            false,
            CancellationToken.None
        );

        var result = await Target.AcquireNextTriggersAsync
        (
            DateTimeOffset.UtcNow,
            2,
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );

        result.Should()
            .HaveCount(1).And
            .ContainSingle(x => x.Description == "Expected");
    }

    [Fact(DisplayName = "If a trigger is acquired Then its state reflects this")]
    public async Task If_a_trigger_is_acquired_Then_its_state_reflects_this()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        var triggerOne = (IOperableTrigger)TriggerBuilder.Create()
            .WithIdentity("Trigger", "Group")
            .StartNow()
            .WithDescription("Expected")
            .ForJob(job)
            .Build();

        triggerOne.ComputeFirstFireTimeUtc(null);
        
        await Target.StoreTriggerAsync
        (
            triggerOne,
            false,
            CancellationToken.None
        );

        await Target.AcquireNextTriggersAsync
        (
            DateTimeOffset.UtcNow,
            2,
            TimeSpan.FromMinutes(1),
            CancellationToken.None
        );

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var trigger = await session.LoadAsync<Trigger>
        (
            Trigger.GetId(Target.InstanceName, "Group", "Trigger")
        );

        trigger.State.Should().Be(InternalTriggerState.Acquired);
    }
}
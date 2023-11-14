using FluentAssertions;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Raven.Client.Documents;

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
        
        var checkJob = await session.LoadAsync<Job>(jobOne.Key.GetDatabaseId());
        var checkTrigger = await session.LoadAsync<Trigger>(triggerOne.Key.GetDatabaseId());

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

    [Fact(DisplayName = "If a job does not exist Then StoreJob persists a new one")]
    public async Task If_a_job_does_not_exist_Then_StoreJob_persists_a_new_one()
    {
        await Target.SchedulerStartedAsync(CancellationToken.None);

        var job = new JobDetailImpl("Job", "Group", typeof(NoOpJob));
        await Target.StoreJobAsync(job, false, CancellationToken.None);

        using var session = Target.DocumentStore!.OpenAsyncSession();
        var checkJob = await session.LoadAsync<Job>(job.Key.GetDatabaseId());

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
        var checkJob = await session.LoadAsync<Job>(job.Key.GetDatabaseId());
        var checkTrigger = await session.LoadAsync<Trigger>(trigger.Key.GetDatabaseId());

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
            JobGroup = job.Group,
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
            JobGroup = "Group",
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);
        
        trigger = new SimpleTriggerImpl("TriggerTwo", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);
        
        trigger = new SimpleTriggerImpl("TriggerTwo", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group,
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
            JobGroup = job.Group,
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);
        
        trigger = new SimpleTriggerImpl("TriggerTwo", "Group")
        {
            JobName = job.Name,
            JobGroup = job.Group,
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
}
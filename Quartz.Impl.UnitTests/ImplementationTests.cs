using FluentAssertions;
using Quartz.Impl.Calendar;
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            JobGroup = job.Group,
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
            var scheduler = await arrangeSession.LoadAsync<Scheduler>(Target.InstanceName);
            scheduler.BlockedJobs.Add("Test");

            await arrangeSession.SaveChangesAsync();
        }

        await Target.ClearAllSchedulingDataAsync(CancellationToken.None);
    
        using var session = Target.DocumentStore!.OpenAsyncSession();
        var check = await session.LoadAsync<Scheduler>(Target.InstanceName);

        check.BlockedJobs.Should().BeEmpty();
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
            DateTimeOffset.UtcNow.AddMinutes(1),
            null,
            SimpleTriggerImpl.RepeatIndefinitely,
            TimeSpan.FromMinutes(1)
        )
        {
            CalendarName = "test"
        };
        await Target.StoreTriggerAsync(trigger, false, CancellationToken.None);

        var calendarStart = trigger.StartTimeUtc.AddHours(1); 

        var calendar = new DailyCalendar(calendarStart.Ticks, calendarStart.AddHours(1).Ticks);
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
}
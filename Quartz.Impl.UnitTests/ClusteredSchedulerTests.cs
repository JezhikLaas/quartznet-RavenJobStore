using FluentAssertions;
using Quartz.Impl.RavenJobStore;
using Quartz.Impl.UnitTests.Helpers;
using Quartz.Impl.UnitTests.Jobs;

namespace Quartz.Impl.UnitTests;

public class ClusteredSchedulerTests : SchedulerTestBase
{
    private List<IScheduler> Schedulers { get; } = new();
    
    public override void Dispose()
    {
        foreach (var scheduler in Schedulers)
        {
            scheduler.Shutdown();
        }

        base.Dispose();
        
        GC.SuppressFinalize(this);
    }

    [Fact(DisplayName = "If multiple schedulers are started Then every scheduler has its own job store")]
    public async Task If_multiple_schedulers_are_started_Then_every_scheduler_has_its_own_job_store()
    {
        Schedulers.Add(await CreateClusteredSchedulerAsync("Test"));
        Schedulers.Add(await CreateClusteredSchedulerAsync("Test"));

        await Schedulers[0].Start();
        await Schedulers[1].Start();

        var storeOne = GetStore(Schedulers[0]);
        var storeTwo = GetStore(Schedulers[1]);

        storeOne.Clustered.Should().BeTrue();
        storeTwo.Clustered.Should().BeTrue();
        
        storeOne.InstanceId.Should().NotBe(storeTwo.InstanceId);
    }

    [Fact(DisplayName = "If multiple triggers reference one job Then a cluster executes only one")]
    public async Task If_multiple_triggers_reference_one_job_Then_a_cluster_executes_only_one()
    {
        Schedulers.Add(await CreateClusteredSchedulerAsync("Test"));
        Schedulers.Add(await CreateClusteredSchedulerAsync("Test"));
        
        var storeOne = GetStore(Schedulers[0]);
        var storeTwo = GetStore(Schedulers[1]);

        var watcherOne = new ControllingWatcher
        (
            storeOne.InstanceId,
            SchedulerExecutionStep.Firing
        );
        var watcherTwo = new ControllingWatcher
        (
            storeTwo.InstanceId,
            SchedulerExecutionStep.Firing
        );
        
        storeOne.DebugWatcher = watcherOne;
        storeTwo.DebugWatcher = watcherTwo;

        await Schedulers[0].Start();
        await Schedulers[1].Start();
        
        var job = new JobDetailImpl("Job", "Group", typeof(NonConcurrentJob), false, false);
        var triggerOne = TriggerBuilder.Create()
            .WithIdentity("Trigger1", "Group")
            .StartNow()
            .WithPriority(1)
            .ForJob(job)
            .Build();
        var triggerTwo = TriggerBuilder.Create()
            .WithIdentity("Trigger2", "Group")
            .StartNow()
            .WithPriority(1)
            .ForJob(job)
            .Build();

        await Schedulers[0].ScheduleJob(job, new[] { triggerOne }, false);
        watcherOne.WaitForEvent(TimeSpan.FromSeconds(10));
        watcherOne.ExecutionStep.Should().Be(SchedulerExecutionStep.Firing);

        await Schedulers[1].ScheduleJob(job, new[] { triggerTwo }, true);
        watcherTwo.WaitForEvent(TimeSpan.FromSeconds(10));
        watcherTwo.ExecutionStep.Should().Be(SchedulerExecutionStep.Firing);
        
        watcherOne.ResetWaitFor(SchedulerExecutionStep.Completing, SchedulerExecutionStep.Releasing);
        watcherTwo.ResetWaitFor(SchedulerExecutionStep.Completing, SchedulerExecutionStep.Releasing);
        
        watcherOne.Continue();
        watcherTwo.Continue();

        watcherOne.WaitForEvent(TimeSpan.FromSeconds(10));
        watcherTwo.WaitForEvent(TimeSpan.FromSeconds(10));

        watcherOne.Continue();
        watcherTwo.Continue();

        var isCompleted = watcherOne.Occurrences[SchedulerExecutionStep.Completing] == 1
                          ||
                          watcherTwo.Occurrences[SchedulerExecutionStep.Completing] == 1;

        isCompleted.Should().BeTrue();

        if (watcherOne.Occurrences[SchedulerExecutionStep.Completing] == 1)
        {
            watcherTwo.Occurrences[SchedulerExecutionStep.Completing].Should().Be(0);
            watcherTwo.Occurrences[SchedulerExecutionStep.Releasing].Should().Be(1);
        }

        if (watcherTwo.Occurrences[SchedulerExecutionStep.Completing] == 1)
        {
            watcherOne.Occurrences[SchedulerExecutionStep.Completing].Should().Be(0);
            watcherOne.Occurrences[SchedulerExecutionStep.Releasing].Should().Be(1);
        }
    }
}

public class ControllingWatcher : IDebugWatcher
{
    public SchedulerExecutionStep? ExecutionStep { get; private set; }

    public IReadOnlyDictionary<SchedulerExecutionStep, int> Occurrences => OccuredEvents; 

    private AutoResetEvent Lock { get; } = new(false);

    private AutoResetEvent WaitingForLock { get; } = new(false);

    private Dictionary<SchedulerExecutionStep, int> OccuredEvents { get; } = new()
    {
        { SchedulerExecutionStep.Firing, 0 },
        { SchedulerExecutionStep.Releasing, 0 },
        { SchedulerExecutionStep.Acquiring, 0 },
        { SchedulerExecutionStep.Completing, 0 }
    };
        
    private string InstanceId { get; }
        
    private IReadOnlyList<SchedulerExecutionStep> WaitFor { get; set; }

    public ControllingWatcher(string id, params SchedulerExecutionStep[] waitFor)
    {
        InstanceId = id;
        WaitFor = waitFor;
    }

    public void ResetWaitFor(params SchedulerExecutionStep[] waitFor)
    {
        WaitFor = waitFor;
    }
        
    public void Notify(SchedulerExecutionStep step, string instanceName, string instanceId)
    {
        if (instanceId != InstanceId) return;
        ++OccuredEvents[step]; 
        
        if (WaitFor.Any() && WaitFor.Contains(step) == false) return;
            
        ExecutionStep = step;
        WaitingForLock.Set();
        Lock.WaitOne();
        ExecutionStep = null;
    }

    public void Continue() => Lock.Set();

    public void WaitForEvent(TimeSpan timeSpan)
    {
        WaitingForLock.WaitOne(timeSpan);
    }
}

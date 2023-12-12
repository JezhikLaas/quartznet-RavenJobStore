using Quartz.Impl.RavenJobStore;

namespace Quartz.Impl.UnitTests.Helpers;

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
        { SchedulerExecutionStep.Completing, 0 },
        { SchedulerExecutionStep.Completed, 0 }
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
        
    public void Notify(SchedulerExecutionStep step, string instanceId)
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
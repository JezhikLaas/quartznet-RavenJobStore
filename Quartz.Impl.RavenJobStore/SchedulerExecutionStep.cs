namespace Quartz.Impl.RavenJobStore;

public enum SchedulerExecutionStep
{
    Acquiring,
    Releasing,
    Firing,
    Completing,
    Completed
}
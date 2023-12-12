namespace Domla.Quartz.Raven;

public enum SchedulerExecutionStep
{
    Acquiring,
    Releasing,
    Firing,
    Completing,
    Completed
}
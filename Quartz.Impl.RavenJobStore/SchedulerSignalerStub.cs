using Quartz;
using Quartz.Spi;

namespace Domla.Quartz.Raven;

public class SchedulerSignalerStub : ISchedulerSignaler
{
    public Task NotifyTriggerListenersMisfired(ITrigger trigger, CancellationToken cancellationToken = new()) =>
        Task.CompletedTask;

    public Task NotifySchedulerListenersFinalized(ITrigger trigger, CancellationToken cancellationToken = new()) =>
        Task.CompletedTask;

    public Task NotifySchedulerListenersJobDeleted(JobKey jobKey, CancellationToken cancellationToken = new()) =>
        Task.CompletedTask;

    public void SignalSchedulingChange(
        DateTimeOffset? candidateNewNextFireTimeUtc,
        CancellationToken cancellationToken = new()) 
    { }

    public Task NotifySchedulerListenersError(
        string message,
        SchedulerException jpe,
        CancellationToken cancellationToken = new()) =>
        Task.CompletedTask;
}
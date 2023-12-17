namespace Quartz.Impl.RavenJobStore.UnitTests.Jobs;

[DisallowConcurrentExecution]
public class SelfReplacingJob : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        var trigger = TriggerBuilder
            .Create()
            .WithIdentity(context.Trigger.Key)
            .WithDescription(context.Trigger.Description)
            .ForJob(context.JobDetail)
            .StartAt(DateBuilder.FutureDate(5, IntervalUnit.Second))
            .WithSchedule(context.Trigger.GetScheduleBuilder())
            .Build();

        await context.Scheduler.RescheduleJob(trigger.Key, trigger, context.CancellationToken).ConfigureAwait(false);
        await context.Scheduler.ResumeJob(context.JobDetail.Key, context.CancellationToken).ConfigureAwait(false);
    }
}
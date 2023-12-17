namespace Quartz.Impl.RavenJobStore.UnitTests.Jobs;

[DisallowConcurrentExecution]
[PersistJobDataAfterExecution]
public class TerminatingJob : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        context.JobDetail.JobDataMap["XXX"] = Random.Shared.Next(1, 10_000).ToString();
        await context
            .Scheduler
            .DeleteJob(context.JobDetail.Key, context.CancellationToken)
            .ConfigureAwait(false);
    }
}
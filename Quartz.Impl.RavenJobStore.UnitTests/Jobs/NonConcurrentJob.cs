namespace Quartz.Impl.RavenJobStore.UnitTests.Jobs;

[DisallowConcurrentExecution]
public class NonConcurrentJob : IJob
{
    public Task Execute(IJobExecutionContext context)
        => Task.CompletedTask;
}

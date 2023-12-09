namespace Quartz.Impl.UnitTests;

[DisallowConcurrentExecution]
public class NonConcurrentJob : IJob
{
    public Task Execute(IJobExecutionContext context)
        => Task.CompletedTask;
}
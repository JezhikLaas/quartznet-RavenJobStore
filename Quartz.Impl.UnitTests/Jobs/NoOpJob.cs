namespace Quartz.Impl.UnitTests.Jobs;

public class NoOpJob : IJob
{
    public Task Execute(IJobExecutionContext context)
        => Task.CompletedTask;
}
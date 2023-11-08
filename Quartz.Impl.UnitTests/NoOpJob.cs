namespace Quartz.Impl.UnitTests;

public class NoOpJob : IJob
{
    public Task Execute(IJobExecutionContext context)
        => Task.CompletedTask;
}
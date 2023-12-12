namespace Quartz.Impl.RavenStore.Example;

[PersistJobDataAfterExecution]
public class EmptyFridge : IJob
{
    private ILogger<EmptyFridge> Logger { get; }

    public EmptyFridge(ILogger<EmptyFridge> logger)
    {
        Logger = logger;
    }
    
    Task IJob.Execute(IJobExecutionContext context)
    {
        Logger.LogInformation("Emptying the fridge...");
        return Task.CompletedTask;
    }
}

[PersistJobDataAfterExecution]
public class TurnOffLights : IJob
{
    private ILogger<EmptyFridge> Logger { get; }

    public TurnOffLights(ILogger<EmptyFridge> logger)
    {
        Logger = logger;
    }
    
    Task IJob.Execute(IJobExecutionContext context)
    {
        Logger.LogInformation("Turning lights off...");
        return Task.CompletedTask;
    }
}

[PersistJobDataAfterExecution]
public class CheckAlive : IJob
{
    private ILogger<EmptyFridge> Logger { get; }

    public CheckAlive(ILogger<EmptyFridge> logger)
    {
        Logger = logger;
    }
    
    Task IJob.Execute(IJobExecutionContext context)
    {
        Logger.LogInformation("Verifying site is up...");
        return Task.CompletedTask;
    }
}

[PersistJobDataAfterExecution]
public class Visit : IJob
{
    private ILogger<EmptyFridge> Logger { get; }

    public Visit(ILogger<EmptyFridge> logger)
    {
        Logger = logger;
    }
    
    Task IJob.Execute(IJobExecutionContext context)
    {
        Logger.LogInformation("Visiting the office, once :)");
        return Task.CompletedTask;
    }
}

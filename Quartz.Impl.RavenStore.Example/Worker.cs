namespace Quartz.Impl.RavenStore.Example;

public class Worker : BackgroundService
{
    private ILogger<Worker> Logger { get; }

    private ISchedulerFactory SchedulerFactory { get; }

    public Worker(ILogger<Worker> logger, ISchedulerFactory schedulerFactory)
    {
        Logger = logger;
        SchedulerFactory = schedulerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Logger.LogInformation("Starting scheduler ...");
        
        var scheduler = await SchedulerFactory.GetScheduler(stoppingToken);
        await scheduler.Start(stoppingToken);

        var emptyFridgeJob = JobBuilder.Create<EmptyFridge>()
            .WithIdentity("EmptyFridgeJob", "Office")
            .RequestRecovery()
            .Build();

        var turnOffLightsJob = JobBuilder.Create<TurnOffLights>()
            .WithIdentity("TurnOffLightsJob", "Office")
            .RequestRecovery()
            .Build();

        var checkAliveJob = JobBuilder.Create<CheckAlive>()
            .WithIdentity("CheckAliveJob", "Office")
            .RequestRecovery()
            .Build();

        var visitJob = JobBuilder.Create<Visit>()
            .WithIdentity("VisitJob", "Office")
            .RequestRecovery()
            .Build();

        // Weekly, Friday at 10 AM (Cron Trigger)
        var emptyFridgeTrigger = TriggerBuilder.Create()
            .WithIdentity("EmptyFridge", "Office")
            .WithCronSchedule("0 0 10 ? * FRI")
            .ForJob("EmptyFridgeJob", "Office")
            .Build();

        // Daily at 6 PM (Daily Interval Trigger)
        var turnOffLightsTrigger = TriggerBuilder.Create()
            .WithIdentity("TurnOffLights", "Office")
            .WithDailyTimeIntervalSchedule(s => s
                .WithIntervalInHours(24)
                .OnEveryDay()
                .StartingDailyAt(TimeOfDay.HourAndMinuteOfDay(18, 0)))
            .Build();

        // Periodic check every 10 seconds (Simple Trigger)
        var checkAliveTrigger = TriggerBuilder.Create()
            .WithIdentity("CheckAlive", "Office")
            .StartAt(DateTime.UtcNow.AddSeconds(3))
            .WithSimpleSchedule(x => x
                .WithIntervalInSeconds(10)
                .RepeatForever())
            .Build();

        var visitTrigger = TriggerBuilder.Create()
            .WithIdentity("Visit", "Office")
            .StartAt(DateTime.UtcNow.AddSeconds(3))
            .Build();
        
        await scheduler.ScheduleJob(checkAliveJob, new [] { checkAliveTrigger }, true, stoppingToken);
        await scheduler.ScheduleJob(emptyFridgeJob, new [] { emptyFridgeTrigger }, true, stoppingToken);
        await scheduler.ScheduleJob(turnOffLightsJob, new [] { turnOffLightsTrigger }, true, stoppingToken);
        await scheduler.ScheduleJob(visitJob, new [] { visitTrigger }, true, stoppingToken);
        
        Logger.LogInformation("Scheduler started, processing jobs now");

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
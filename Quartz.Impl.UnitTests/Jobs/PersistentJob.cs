namespace Quartz.Impl.UnitTests.Jobs;

[PersistJobDataAfterExecution]
public class PersistentJob : IJob
{
    public string? TestProperty { get; set; }
    
    public Task Execute(IJobExecutionContext context)
    {
        if (string.IsNullOrEmpty(TestProperty))
        {
            context.MergedJobDataMap[nameof(TestProperty)] = "Failed";
        }
        else
        {
            context.JobDetail.JobDataMap[nameof(TestProperty)] = "Ok";
        }

        return Task.CompletedTask;
    }
}
namespace Quartz.Impl.RavenJobStore.UnitTests.Jobs;

[PersistJobDataAfterExecution]
[DisallowConcurrentExecution]
public class PersistentJob : IJob
{
    public string? TestProperty { get; set; }
    
    public Task Execute(IJobExecutionContext context)
    {
        if (string.IsNullOrEmpty(TestProperty))
        {
            context.JobDetail.JobDataMap[nameof(TestProperty)] = "Failed";
        }
        else
        {
            context.JobDetail.JobDataMap[nameof(TestProperty)] = "Ok";
        }

        return Task.CompletedTask;
    }
}
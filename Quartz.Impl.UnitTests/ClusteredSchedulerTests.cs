using FluentAssertions;
using Quartz.Impl.UnitTests.Helpers;

namespace Quartz.Impl.UnitTests;

public class ClusteredSchedulerTests : SchedulerTestBase
{
    private List<IScheduler> Schedulers { get; } = new();
    
    public override void Dispose()
    {
        foreach (var scheduler in Schedulers)
        {
            scheduler.Shutdown();
        }

        base.Dispose();
        
        GC.SuppressFinalize(this);
    }

    [Fact(DisplayName = "If multiple schedulers are started Then every scheduler has its own job store")]
    public async Task If_multiple_schedulers_are_started_Then_every_scheduler_has_its_own_job_store()
    {
        Schedulers.Add(await CreateClusteredSchedulerAsync("Test"));
        Schedulers.Add(await CreateClusteredSchedulerAsync("Test"));

        await Schedulers[0].Start();
        await Schedulers[1].Start();

        var storeOne = GetStore(Schedulers[0]);
        var storeTwo = GetStore(Schedulers[1]);

        storeOne.Clustered.Should().BeTrue();
        storeTwo.Clustered.Should().BeTrue();
        
        storeOne.InstanceId.Should().NotBe(storeTwo.InstanceId);
    }
}
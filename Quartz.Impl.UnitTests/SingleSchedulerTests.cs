using FluentAssertions;
using Quartz.Impl.RavenJobStore.Entities;
using Quartz.Impl.UnitTests.Helpers;

namespace Quartz.Impl.UnitTests;

public class SingleSchedulerTests : SchedulerTestBase
{
    private IScheduler? Scheduler { get; set; }

    public override void Dispose()
    {
        Scheduler?.Shutdown();
        base.Dispose();
        
        GC.SuppressFinalize(this);
    }
    
    [Fact(DisplayName = "If a scheduler is created with type raven Then a RavenJobStore gets instantiated")]
    public async Task If_a_scheduler_is_created_with_type_raven_Then_a_RavenJobStore_gets_instantiated()
    {
        Scheduler = await CreateScheduler("Test");
        await Scheduler.Start();
        
        var ravenJobStore = GetStore(Scheduler);
        ravenJobStore.Should().NotBeNull();
    }
    
    [Fact(DisplayName = "If a collection name is used Then documents are placed within it")]
    public async Task If_a_collection_name_is_used_Then_documents_are_placed_within_it()
    {
        Scheduler = await CreateScheduler("Test", collectionName: "SchedulerData");
        await Scheduler.Start();
        
        using var session = DocumentStore.OpenAsyncSession();
        var scheduler = await session.LoadAsync<Scheduler>("Test");
        var meta = session.Advanced.GetMetadataFor(scheduler);

        meta.Should().Contain(x => x.Key == "@collection" && x.Value.Equals("SchedulerData"));
    }
}
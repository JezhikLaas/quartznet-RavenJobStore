using System.Collections.Specialized;
using System.Globalization;
using FluentAssertions;
using Quartz.Impl.RavenJobStore.Entities;
using Raven.Client.Documents;
using Xunit.Abstractions;

namespace Quartz.Impl.UnitTests;

public class SchedulerTests : TestBase
{
    private IDocumentStore DocumentStore { get; }
    
    private IScheduler? Scheduler { get; set; }
    
    private ITestOutputHelper Output { get; }

    public SchedulerTests(ITestOutputHelper output)
    {
        DocumentStore = CreateStore();
        Output = output;
    }

    public override void Dispose()
    {
        Scheduler?.Shutdown();
        DocumentStore.Dispose();
        base.Dispose();
        
        GC.SuppressFinalize(this);
    }
    
    [Fact(DisplayName = "If a scheduler is created with type raven Then a RavenJobStore gets instantiated")]
    public async Task If_a_scheduler_is_created_with_type_raven_Then_a_RavenJobStore_gets_instantiated()
    {
        Scheduler = await CreateScheduler("Test");
        await Scheduler.Start();
        
        using var session = DocumentStore.OpenAsyncSession();
        var schedulerExists = await session.Advanced.ExistsAsync("Test");

        schedulerExists.Should().BeTrue();
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
    
    private Task<IScheduler> CreateScheduler(string name, int threadCount = 5, string? collectionName = null)
    {
        var address = DocumentStore.Urls.First();
        var database = DocumentStore.Database;
        
        var properties = new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = name,
            ["quartz.scheduler.instanceId"] = "AUTO",
            ["quartz.threadPool.threadCount"] = threadCount.ToString(CultureInfo.InvariantCulture),
            ["quartz.serializer.type"] = "binary",
            ["quartz.jobStore.type"] = "Quartz.Impl.RavenJobStore.RavenJobStore, Quartz.Impl.RavenJobStore",
            ["quartz.jobStore.urls"] = $"[\"{address}\"]",
            ["quartz.jobStore.database"] = database,
            ["quartz.jobStore.collectionName"] = collectionName
        };

        var stdSchedulerFactory = new StdSchedulerFactory(properties);
        return stdSchedulerFactory.GetScheduler();
    }
 
}
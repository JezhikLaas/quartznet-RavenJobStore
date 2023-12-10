using System.Collections.Specialized;
using System.Globalization;
using System.Reflection;
using Quartz.Core;
using Raven.Client.Documents;

namespace Quartz.Impl.UnitTests.Helpers;

public abstract class SchedulerTestBase : TestBase
{
    protected IDocumentStore DocumentStore { get; }

    protected SchedulerTestBase()
    {
        DocumentStore = CreateStore();
    }
    
    public override void Dispose()
    {
        DocumentStore.Dispose();
        base.Dispose();
        
        GC.SuppressFinalize(this);
    }
    
    protected Task<IScheduler> CreateSingleSchedulerAsync(
        string name,
        int threadCount = 5,
        string? collectionName = null) =>
        CreateSchedulerAsync(CreateSingleProperties(name, threadCount, collectionName));

    protected Task<IScheduler> CreateClusteredSchedulerAsync(
        string name,
        int threadCount = 5,
        string? collectionName = null) =>
        CreateSchedulerAsync(CreateClusteredProperties(name, threadCount, collectionName));

    private Task<IScheduler> CreateSchedulerAsync(NameValueCollection properties)
    {
        var schedulerFactory = new TestSchedulerFactory(properties);
        return schedulerFactory.GetScheduler();
    }

    private NameValueCollection CreateClusteredProperties(
        string name,
        int threadCount,
        string? collectionName)
    {
        var properties = CreateSingleProperties
        (
            name,
            threadCount,
            collectionName
        );

        // To force a unique instance id event on lightning fast super computers
        Thread.Sleep(10);
        
        properties.Add("quartz.jobStore.clustered", "true");
        properties["quartz.scheduler.instanceId"] = $"CLUSTER{DateTime.Now.Ticks}";

        return properties;
    }

    private NameValueCollection CreateSingleProperties(
        string name,
        int threadCount,
        string? collectionName)
    {
        var address = DocumentStore.Urls.First();
        var database = DocumentStore.Database;
        return new NameValueCollection
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
    }

    /// <summary>
    /// This is only for testing - never try to use something like this in production code.
    /// </summary>
    /// <returns></returns>
    protected RavenJobStore.RavenJobStore GetStore(IScheduler scheduler)
    {
        var realSchedulerField = scheduler
            .GetType()
            .GetField("sched", BindingFlags.Instance | BindingFlags.NonPublic);

        var realScheduler = (QuartzScheduler)realSchedulerField!.GetValue(scheduler)!;
        var resourcesField = realScheduler
            .GetType()
            .GetField("resources", BindingFlags.Instance | BindingFlags.NonPublic);

        var resources = (QuartzSchedulerResources)resourcesField!.GetValue(realScheduler)!;

        return (RavenJobStore.RavenJobStore)resources.JobStore;
    }
}

public class TestSchedulerFactory : StdSchedulerFactory
{
    public TestSchedulerFactory(NameValueCollection properties)
        : base(properties)
    { }
    
    public override async Task<IScheduler> GetScheduler(CancellationToken cancellationToken = new CancellationToken())
    {
        var instantiateMethod = typeof(StdSchedulerFactory).GetMethod
        (
            "Instantiate",
            BindingFlags.Instance | BindingFlags.NonPublic,
            Type.EmptyTypes
        );

        var task = (Task<IScheduler>)instantiateMethod!.Invoke(this, null)!;
        await task.ConfigureAwait(false);
        
        return task.Result;
    }
}
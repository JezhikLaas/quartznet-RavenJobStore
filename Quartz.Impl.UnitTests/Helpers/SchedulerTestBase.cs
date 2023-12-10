using System.Collections.Specialized;
using System.Globalization;
using System.Reflection;
using Quartz.Core;
using Quartz.Impl.RavenJobStore.Entities;
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
    
    protected Task<IScheduler> CreateScheduler(string name, int threadCount = 5, string? collectionName = null)
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

    /// <summary>
    /// This is only for testing - never try to use something like this in production code.
    /// </summary>
    /// <returns></returns>
    protected RavenJobStore.RavenJobStore? GetStore(IScheduler scheduler)
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
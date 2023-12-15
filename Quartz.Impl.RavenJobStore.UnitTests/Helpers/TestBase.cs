using Raven.Client.Documents;
using Raven.TestDriver;

namespace Quartz.Impl.RavenJobStore.UnitTests.Helpers;

public abstract class TestBase : RavenTestDriver
{
    static TestBase()
    {
        ConfigureServer(new TestServerOptions
        {
            FrameworkVersion = "7.0.x",
            GracefulShutdownTimeout = TimeSpan.FromMinutes(1)
        });
    }

    protected override void PreInitialize(IDocumentStore documentStore)
    {
        documentStore.Conventions.UseOptimisticConcurrency = true;
    }

    protected IDocumentStore CreateStore()
    {
        var result = GetDocumentStore();
        return result;
    }
}
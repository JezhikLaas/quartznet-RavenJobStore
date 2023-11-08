using Raven.Client.Documents;
using Raven.TestDriver;

namespace Quartz.Impl.UnitTests;

public class TestBase : RavenTestDriver
{
    static TestBase()
    {
        ConfigureServer(new TestServerOptions
        {
            FrameworkVersion = "7.0.x",
            GracefulShutdownTimeout = TimeSpan.FromMinutes(1)
        });
    }

    protected IDocumentStore CreateStore()
    {
        var result = GetDocumentStore();
        return result;
    }

    protected override void PreInitialize(IDocumentStore documentStore)
    {
        documentStore.OnBeforeQuery += (_, beforeQueryExecutedArgs) =>
        {
            beforeQueryExecutedArgs.QueryCustomization.WaitForNonStaleResults();
        };
    }
}
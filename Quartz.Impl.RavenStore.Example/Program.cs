using Quartz;
using Quartz.Impl.RavenJobStore;
using Quartz.Impl.RavenStore.Example;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddHostedService<Worker>();
        services.AddQuartz(quartz =>
        {
            quartz.SetProperty("quartz.serializer.type", "binary"); 
            quartz.UseDefaultThreadPool(pool => pool.MaxConcurrency = 10);
            quartz.UsePersistentStore(store =>
            {
                store.UseRavenDb(configuration =>
                {
                    // To run this demo you need to have running RavenDB at http://localhost:8080.
                    // Or use a different URL which suits your needs.
                    configuration.Urls = new[] { "http://localhost:8080" };
                    
                    // The database is expected to be created.
                    configuration.Database = "QuartzDemo";
                });
            });
        });
        services.AddQuartzHostedService(quartz => quartz.WaitForJobsToComplete = true);
    })
    .Build()
    .UseRavenJobStoreLogging();

host.Run();
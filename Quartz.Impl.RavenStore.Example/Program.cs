using Domla.Quartz.Raven;
using Quartz;
using Quartz.Impl.RavenStore.Example;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;

// CreateFromProperties().Run();
CreateWithOwnDocumentStore().Run();

#pragma warning disable CS8321 // Local function is declared but never used
// For documentation purposes.
IHost CreateWithOwnDocumentStore() =>
#pragma warning restore CS8321 // Local function is declared but never used
    Host.CreateDefaultBuilder(args)
        .ConfigureServices(services =>
        {
            // Build and register a Raven document store as usual.
            var conventions = new DocumentConventions
            {
                UseOptimisticConcurrency = true
            };
            var documentStore = new DocumentStore
            {
                Conventions = conventions,
                // To run this demo you need to have running RavenDB at http://localhost:8080.
                // Or use a different URL which suits your needs.
                Urls = new[] { "http://localhost:8080" },
                    
                // The database is expected to be created.
                Database = "QuartzDemo"
            };

            documentStore.Initialize();

            services.AddSingleton<IDocumentStore>(documentStore);

            services.AddHostedService<Worker>();

            services.AddQuartz(quartz =>
            {
                // Quartz insists on selecting a serializer. 
                quartz.SetProperty("quartz.serializer.type", "binary");

                // Configure thread pool size for Quartz,
                // not directly to RavenJobStore.
                quartz.UseDefaultThreadPool(pool => pool.MaxConcurrency = 10);

                // Yes, it is that simple.
                quartz.UsePersistentStore(store => store.UseRavenDb(services));
            });

            services.AddQuartzHostedService(quartz => quartz.WaitForJobsToComplete = true);
        })
        .Build()
        .UseRavenJobStoreLogging();

#pragma warning disable CS8321 // Local function is declared but never used
// For documentation purposes.
IHost CreateFromProperties() =>
#pragma warning restore CS8321 // Local function is declared but never used
    Host.CreateDefaultBuilder(args)
        .ConfigureServices(services =>
        {
            services.AddHostedService<Worker>();

            services.AddQuartz(quartz =>
            {
                // Quartz insists on selecting a serializer. 
                quartz.SetProperty("quartz.serializer.type", "binary");

                // Configure thread pool size for Quartz,
                // not directly to RavenJobStore.
                quartz.UseDefaultThreadPool(pool => pool.MaxConcurrency = 10);

                quartz.UsePersistentStore(store =>
                {
                    // Tell Quartz to use RavenJobStore as store backend,
                    // creating a DocumentStore along the way.
                    store.UseRavenDb(configuration =>
                    {
                        // To run this demo you need to have running RavenDB at http://localhost:8080.
                        // Or use a different URL which suits your needs.
                        configuration.Urls = new[] { "http://localhost:8080" };

                        // The database is expected to be created.
                        configuration.Database = "QuartzDemo";

                        // Just for the record - you can prefix the collection 
                        // names for our classes with a common name.
                        // configuration.CollectionName = "Name";

                        // Path to a raven client certificate
                        // configuration.CertPath = "certificate.pem";

                        // Password for the certificate.
                        // configuration.CertPass = "secret-password";
                    });
                });
            });
            services.AddQuartzHostedService(quartz => quartz.WaitForJobsToComplete = true);
        })
        .Build()
        .UseRavenJobStoreLogging();

[![build and test](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/build-and-test.yml)

[![Qodana](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/code_quality.yml/badge.svg)](https://github.com/JezhikLaas/quartznet-RavenJobStore/actions/workflows/code_quality.yml)


# Quartz.NET-RavenJobStore
JobStore implementation for Quartz.NET scheduler using RavenDB.<br>
Not exactly a fork of, but inspired by [Quartz.NET-RavenDB](https://github.com/ravendb/quartznet-RavenDB)

First prerelease available on nuget.

## About

[Quartz.NET](https://github.com/quartznet/quartznet) is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

[Quartz.NET on RavenDB](https://github.com/JezhikLaas/quartznet-RavenJobStore) is a provider written for Quartz.NET which lets us use the [RavenDB](https://ravendb.net/features) NoSQL database as the persistent Job Store for scheduling data (instead of the SQL solutions that are built-in Quartz.NET).

## Project goals
* Can handle high volumes of jobs and triggers.
* Suitable for clustered schedulers.
* Different schedulers (instance names) can share the same database.

## How to get started
First add scheduling to your app using Quartz.NET ([example](http://www.quartz-scheduler.net/documentation/quartz-2.x/quick-start.html)).
Then install the NuGet [package](https://www.nuget.org/packages/Domla.Quartz.RavenJobStore/).

You can either let the package create a IDocumentStore on its own:

```csharp

using Domla.Quartz.Raven;
using Quartz;
using Quartz.Impl.RavenStore.Example;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;

CreateFromProperties().Run();

IHost CreateFromProperties() =>
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
        .Build();

```

or you can create and configure your own IDocumentStore and let the JobStore use it:

```csharp

using Domla.Quartz.Raven;
using Quartz;
using Quartz.Impl.RavenStore.Example;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;

CreateWithOwnDocumentStore().Run();

IHost CreateWithOwnDocumentStore() =>
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

```

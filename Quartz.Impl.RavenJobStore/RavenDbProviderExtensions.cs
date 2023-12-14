using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;
using Quartz.Impl;
using Quartz.Spi;
using Quartz.Util;

namespace Domla.Quartz.Raven;

[SuppressMessage("ReSharper", "UnusedMember.Global")]
// ReSharper disable once UnusedType.Global
public static class RavenDbProviderExtensions
{
    /// <summary>
    /// Configures the persistent store options to use RavenDB as the job store.
    /// Use this one if the IDocumentStore shall be created by <see cref="RavenJobStore"/>.
    /// </summary>
    /// <param name="options">The persistent store options.</param>
    /// <param name="config">The configuration action for RavenDB provider options.</param>
    public static void UseRavenDb(
        this SchedulerBuilder.PersistentStoreOptions options,
        Action<RavenDbProviderOptions> config)
    {
        options.SetProperty
        (
            StdSchedulerFactory.PropertyJobStoreType,
            typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion()
        );
        
        config.Invoke(new RavenDbProviderOptions(options));
    }

    /// <summary>
    /// Configures the scheduler to use RavenDB as the persistent store.
    /// To be used if a IDocumentStore should be fetched from DI.
    /// </summary>
    /// <param name="options">The persistent store options.</param>
    /// <param name="services">The service collection.</param>
    public static void UseRavenDb(
        this SchedulerBuilder.PersistentStoreOptions options,
        IServiceCollection services)
    {
        options.SetProperty
        (
            StdSchedulerFactory.PropertyJobStoreType,
            typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion()
        );

        services.AddSingleton<IJobStore, RavenJobStore>();
    }

    /// <summary>
    /// Adds RavenJobStore logging to the application's request pipeline.
    /// </summary>
    /// <param name="builder">The <see cref="IApplicationBuilder"/> instance.</param>
    /// <returns>The <see cref="IApplicationBuilder"/> instance with RavenJobStore logging added.</returns>
    public static IApplicationBuilder UseRavenJobStoreLogging(this IApplicationBuilder builder)
    {
        RavenJobStore.LoggerFactory = builder.ApplicationServices.GetService<ILoggerFactory>();

        return builder;
    }

    /// <summary>
    /// Configures the host to use RavenJobStore's logging functionality.
    /// </summary>
    /// <param name="host">The host to configure.</param>
    /// <returns>The configured host.</returns>
    public static IHost UseRavenJobStoreLogging(this IHost host)
    {
        RavenJobStore.LoggerFactory = host.Services.GetService<ILoggerFactory>(); 

        return host;
    }
}
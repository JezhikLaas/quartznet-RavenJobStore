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

    public static void UseRavenJobStoreLogging(this IApplicationBuilder builder)
    {
        var factory = builder.ApplicationServices.GetService<ILoggerFactory>();
        if (factory == null) return;

        if (builder.ApplicationServices.GetService<IJobStore>() is RavenJobStore store)
        {
            store.Logger = new Logger<RavenJobStore>(factory);
        }
    }

    public static IHost UseRavenJobStoreLogging(this IHost host)
    {
        var factory = host.Services.GetService<ILoggerFactory>();
        if (factory == null) return host;

        if (host.Services.GetService<IJobStore>() is RavenJobStore store)
        {
            store.Logger = new Logger<RavenJobStore>(factory);
        }

        return host;
    }
}
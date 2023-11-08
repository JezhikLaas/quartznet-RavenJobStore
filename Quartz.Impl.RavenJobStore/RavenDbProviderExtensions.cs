using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz.Spi;
using Quartz.Util;

namespace Quartz.Impl.RavenJobStore;

[SuppressMessage("ReSharper", "UnusedMember.Global")]
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

        if (factory == null || RavenJobStore.Instance == null) return;
        RavenJobStore.Instance.Logger = new Logger<RavenJobStore>(factory);
    }

    public static void UseRavenJobStoreLogging(this IHost host)
    {
        var factory = host.Services.GetService<ILoggerFactory>();

        if (factory == null || RavenJobStore.Instance == null) return;
        RavenJobStore.Instance.Logger = new Logger<RavenJobStore>(factory);
    }
}
using System.Text.Json;

namespace Quartz.Impl.RavenJobStore;

public class RavenDbProviderOptions
{
    private readonly SchedulerBuilder.PersistentStoreOptions _options;

    protected internal RavenDbProviderOptions(SchedulerBuilder.PersistentStoreOptions options)
    {
        _options = options;
    }

    /// <summary>
    ///     The default database to use for the scheduler data.
    /// </summary>
    public string Database
    {
        set => _options.SetProperty("quartz.jobStore.database", value);
    }

    /// <summary>
    ///     The URL(s) to one or more database servers.
    /// </summary>
    public string[] Urls
    {
        set => _options.SetProperty("quartz.jobStore.urls", JsonSerializer.Serialize(value));
    }

    /// <summary>
    ///     Optional certificate path for authentication.
    /// </summary>
    public string CertPath
    {
        set => _options.SetProperty("quartz.jobStore.certPath", value);
    }

    /// <summary>
    ///     Optional certificate password.
    /// </summary>
    public string CertPass
    {
        set => _options.SetProperty("quartz.jobStore.certPass", value);
    }
}
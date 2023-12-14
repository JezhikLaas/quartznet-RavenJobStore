using System.Text.Json;
using Quartz;

namespace Domla.Quartz.Raven;

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
    // ReSharper disable once UnusedMember.Global
    public string Database
    {
        set => _options.SetProperty("quartz.jobStore.database", value);
    }

    /// <summary>
    ///     The URL(s) to one or more database servers.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public string[] Urls
    {
        set => _options.SetProperty("quartz.jobStore.urls", JsonSerializer.Serialize(value));
    }

    /// <summary>
    ///     Optional certificate path for authentication.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public string CertPath
    {
        set => _options.SetProperty("quartz.jobStore.certPath", value);
    }

    /// <summary>
    ///     Optional certificate password.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public string CertPass
    {
        set => _options.SetProperty("quartz.jobStore.certPass", value);
    }

    /// <summary>
    /// Number of retries in the case of concurrency exception. Default is 100.
    /// Do not set to a low value if you plan to use a cluster of schedulers.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public int ConcurrencyErrorRetries
    {
        set => _options.SetProperty("quartz.jobStore.concurrencyErrorRetries", value.ToString());
    }

    /// <summary>
    /// RavenDB needs some time to process the indexes after adding / modifying
    /// a document. This sets the maximum wait time for indexes to become
    /// up to date before performing a query. The default is 15 seconds.
    /// </summary>
    public int SecondsToWaitForIndexing
    {
        set => _options.SetProperty("quartz.jobStore.secondsToWaitForIndexing", value.ToString());
    }

    /// <summary>
    ///     Optional collection name to store all scheduler data in.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public string CollectionName
    {
        set => _options.SetProperty("quartz.jobStore.collectionName", value);
    }
}
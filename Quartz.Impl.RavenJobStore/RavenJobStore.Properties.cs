using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using Quartz;
using Quartz.Spi;
using Raven.Client.Documents;
using JsonSerializer = System.Text.Json.JsonSerializer;

// ReSharper disable MemberCanBePrivate.Global

namespace Domla.Quartz.Raven;

public partial class RavenJobStore
{
    private static long _fireTimeCounter = SystemTime.UtcNow().Ticks;

    private static TimeSpan MisfireThresholdValue { get; set; } = TimeSpan.FromSeconds(5);

    private ISchedulerSignaler Signaler { get; set; } = new SchedulerSignalerStub();

    private int _secondsToWaitForIndexing = 15;

    private int _concurrencyErrorRetries = 100;
    
    private string? _database;
    
    internal static ILoggerFactory? LoggerFactory { get; set; } 
    
    private string[]? RavenNodes { get; set; }

    /// <summary>
    ///     The database to use for this <see cref="RavenJobStore" /> instance.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public string? Database
    {
        get => _database;
        set => _database = value ?? throw new ArgumentException("must not be null", nameof(value));
    }

    /// <summary>
    /// Only here to satisfy the object creation. We always attempt to (de-)serialize any
    /// value type in the Job Data Map anyway, not just strings.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public bool UseProperties { get; set; }

    /// <summary>
    /// Gets the URL(s) to the database server(s).
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public string Urls
    {
        get => RavenNodes != null
            ? JsonSerializer.Serialize(RavenNodes)
            : string.Empty;
        set => RavenNodes = JsonSerializer.Deserialize<string[]>(value);
    }

    /// <summary>
    /// Gets the path to the certificate to authenticate against the database.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Gets the password to the certificate to authenticate against the database.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets the current configured <see cref="IDocumentStore" />.
    /// </summary>
    internal IDocumentStore? DocumentStore { get; set; }

    /// <summary>
    /// The collection name to be used for scheduler data. />.
    /// </summary>
    [JsonProperty]
    public string? CollectionName { get; set; }

    // ReSharper disable once UnusedMember.Global
    protected static DateTimeOffset MisfireTime
    {
        [MethodImpl(MethodImplOptions.Synchronized)]
        get
        {
            var misfireTime = SystemTime.UtcNow();
            misfireTime = MisfireThreshold > TimeSpan.Zero
                ? misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds)
                : misfireTime;

            return misfireTime;
        }
    }

    [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
    public static TimeSpan MisfireThreshold
    {
        [MethodImpl(MethodImplOptions.Synchronized)]
        get => MisfireThresholdValue;
        
        [MethodImpl(MethodImplOptions.Synchronized)]
        // ReSharper disable once UnusedMember.Global
        set
        {
            if (value.TotalMilliseconds < 1) throw new ArgumentException("MisfireThreshold must be larger than 0");
            MisfireThresholdValue = value;
        }
    }

    public bool SupportsPersistence => true;

    public long EstimatedTimeToReleaseAndAcquireTrigger => 100;

    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    public bool Clustered { get; set; }

    public string InstanceId { get; set; } = "InstanceId";

    public string InstanceName { get; set; } = "InstanceName";

    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public int ConcurrencyErrorRetries
    {
        get => _concurrencyErrorRetries;
        set => _concurrencyErrorRetries = value.MustBeGreaterThan(0);
    }

    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public int SecondsToWaitForIndexing
    {
        get => _secondsToWaitForIndexing;
        set => _secondsToWaitForIndexing = value.MustBeGreaterThanOrEqualTo(0);
    }

    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    public int ThreadPoolSize { get; set; }

    internal ILogger<RavenJobStore> Logger { get; set; } = NullLoggerFactory.Instance.CreateLogger<RavenJobStore>();
    
    internal IDebugWatcher? DebugWatcher { get; set; }
}
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using Quartz.Spi;
using Raven.Client.Documents;
using JsonSerializer = System.Text.Json.JsonSerializer;

// ReSharper disable MemberCanBePrivate.Global

namespace Quartz.Impl.RavenJobStore;

public partial class RavenJobStore
{
    private static long _fireTimeCounter = SystemTime.UtcNow().Ticks;

    private static TimeSpan MisfireThresholdValue { get; set; } = TimeSpan.FromSeconds(5);

    private ISchedulerSignaler Signaler { get; set; } = new SchedulerSignalerStub();
    
    private string[]? RavenNodes { get; set; } 

    /// <summary>
    ///     The database to use for this <see cref="RavenJobStore" /> instance.
    /// </summary>
    [JsonProperty]
    public string? Database { get; set; }

    /// <summary>
    /// Only here to satisfy the object creation. We always attempt to (de-)serialize any
    /// value type in the Job Data Map anyway, not just strings.
    /// </summary>
    [JsonProperty]
    public bool UseProperties { get; set; }

    /// <summary>
    /// Gets the URL(s) to the database server(s).
    /// </summary>
    [JsonProperty]
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
    [JsonProperty]
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Gets the password to the certificate to authenticate against the database.
    /// </summary>
    [JsonProperty]
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets the current configured <see cref="IDocumentStore" />.
    /// </summary>
    [JsonProperty]
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

    [JsonProperty]
    public bool Clustered { get; set; }

    [JsonProperty]
    public string InstanceId { get; set; } = "InstanceId";

    public string InstanceName { get; set; } = "InstanceName";

    [JsonProperty]
    public int ThreadPoolSize { get; set; }

    internal ILogger<RavenJobStore> Logger { get; set; } = NullLoggerFactory.Instance.CreateLogger<RavenJobStore>();
    
    internal IDebugWatcher? DebugWatcher { get; set; }
}
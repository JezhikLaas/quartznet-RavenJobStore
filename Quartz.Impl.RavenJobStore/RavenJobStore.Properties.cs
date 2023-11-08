using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Quartz.Spi;
using Raven.Client.Documents;
// ReSharper disable MemberCanBePrivate.Global

namespace Quartz.Impl.RavenJobStore;

public partial class RavenJobStore
{
    private static long _fireTimeCounter = SystemTime.UtcNow().Ticks;

    private static TimeSpan MisfireThresholdValue { get; set; } = TimeSpan.FromSeconds(5);

    private ISchedulerSignaler Signaler { get; set; } = null!;
    
    private string[]? RavenNodes { get; set; } 

    /// <summary>
    ///     The database to use for this <see cref="RavenJobStore" /> instance.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// Only here to satisfy the object creation. We always attempt to (de-)serialize any
    /// value type in the Job Data Map anyway, not just strings.
    /// </summary>
    public bool UseProperties { get; set; }

    /// <summary>
    /// Gets the URL(s) to the database server(s).
    /// </summary>
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
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Gets the password to the certificate to authenticate against the database.
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets the current configured <see cref="IDocumentStore" />.
    /// </summary>
    internal IDocumentStore? DocumentStore { get; set; }

    protected DateTimeOffset MisfireTime
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
    private TimeSpan MisfireThreshold
    {
        [MethodImpl(MethodImplOptions.Synchronized)]
        get => MisfireThresholdValue;
        
        [MethodImpl(MethodImplOptions.Synchronized)]
        set
        {
            if (value.TotalMilliseconds < 1) throw new ArgumentException("MisfireThreshold must be larger than 0");
            MisfireThresholdValue = value;
        }
    }

    public bool SupportsPersistence => true;

    public long EstimatedTimeToReleaseAndAcquireTrigger => 100;

    public bool Clustered => false;

    public string InstanceId { get; set; } = "instance_two";

    public string InstanceName { get; set; } = "UnitTestScheduler";

    public int ThreadPoolSize { get; set; }

    internal ILogger<RavenJobStore> Logger { get; set; } = NullLoggerFactory.Instance.CreateLogger<RavenJobStore>();
}
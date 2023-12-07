using System.Text.Json;
using Newtonsoft.Json;
using Quartz.Util;

namespace Quartz.Impl.RavenJobStore;

internal class Job
{
    public Job(IJobDetail? newJob, string schedulerInstanceName)
    {
        if (newJob == null) return;

        Name = newJob.Key.Name;
        Group = newJob.Key.Group;
        Scheduler = schedulerInstanceName;

        Description = newJob.Description;
        JobType = newJob.JobType;
        Durable = newJob.Durable;
        ConcurrentExecutionDisallowed = newJob.ConcurrentExecutionDisallowed;
        PersistJobDataAfterExecution = newJob.PersistJobDataAfterExecution;
        RequestsRecovery = newJob.RequestsRecovery;
        JobDataMap = new Dictionary<string, object>(newJob.JobDataMap.WrappedMap);
    }

    [JsonProperty]
    public string Name { get; set; } = string.Empty;

    [JsonProperty]
    public string Group { get; set; } = string.Empty;
    
    public string Key => $"{Name}/{Group}";

    [JsonProperty]
    public string Scheduler { get; set; } = string.Empty;

    [JsonProperty]
    public string? Description { get; set; }
    
    [JsonProperty]
    [System.Text.Json.Serialization.JsonConverter(typeof(JobTypeConverter))]
    public Type? JobType { get; set; }
    
    [JsonProperty]
    public bool Durable { get; set; }
    
    [JsonProperty]
    public bool ConcurrentExecutionDisallowed { get; set; }
    
    [JsonProperty]
    public bool PersistJobDataAfterExecution { get; set; }
    
    [JsonProperty]
    public bool RequestsRecovery { get; set; }
    
    public IDictionary<string, object>? JobDataMap { get; set; }

    [System.Text.Json.Serialization.JsonIgnore]
    public JobKey JobKey =>
        new(Name, Group);

    /// <summary>
    ///     Converts this <see cref="Job"/> back into an <see cref="IJobDetail"/>.
    /// </summary>
    /// <returns>The built <see cref="IJobDetail"/>.</returns>
    public IJobDetail Deserialize()
    {
        return JobBuilder.Create()
            .WithIdentity(Name, Group)
            .WithDescription(Description)
            .OfType(JobType!)
            .RequestRecovery(RequestsRecovery)
            .SetJobData(new JobDataMap(JobDataMap ?? new Dictionary<string, object>()))
            .StoreDurably(Durable)
            .Build();

        // A JobDetail doesn't have builder methods for two properties:   IsNonConcurrent,IsUpdateData
        // they are determined according to attributes on the job class
    }
}

public class JobTypeConverter : System.Text.Json.Serialization.JsonConverter<Type>
{
    public override Type Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, Type value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.AssemblyQualifiedNameWithoutVersion());
    }
}

internal static class JobKeyExtensions
{
    public static string GetDatabaseId(this JobKey? jobKey) => 
        $"{jobKey?.Name ?? string.Empty}/{jobKey?.Group ?? string.Empty}";
}

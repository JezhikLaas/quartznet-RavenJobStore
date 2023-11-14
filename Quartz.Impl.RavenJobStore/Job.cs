using System.Text.Json;
using System.Text.Json.Serialization;
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

    public string Name { get; set; } = string.Empty;

    public string Group { get; set; } = string.Empty;
    
    public string Key => $"{Name}/{Group}";

    public string Scheduler { get; set; } = string.Empty;

    public string? Description { get; set; }
    
    [JsonConverter(typeof(JobTypeConverter))]
    public Type? JobType { get; set; }
    
    public bool Durable { get; set; }
    
    public bool ConcurrentExecutionDisallowed { get; set; }
    
    public bool PersistJobDataAfterExecution { get; set; }
    
    public bool RequestsRecovery { get; set; }
    
    public IDictionary<string, object>? JobDataMap { get; set; }

    [JsonIgnore]
    public JobKey JobKey =>
        new JobKey(Name, Group);

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

public class JobTypeConverter : JsonConverter<Type>
{
    public override Type? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
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

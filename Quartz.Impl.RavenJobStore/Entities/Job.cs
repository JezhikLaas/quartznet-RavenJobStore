using Newtonsoft.Json;
using Quartz;

namespace Domla.Quartz.Raven.Entities;

internal class Job : SerializeQuartzData, IGroupedElement
{
    public Job(IJobDetail? job, string schedulerInstanceName)
    {
        if (job == null) return;

        Name = job.Key.Name;
        Group = job.Key.Group;
        Scheduler = schedulerInstanceName;

        Id = GetId(Scheduler, Group, Name);

        Item = job;

        Description = job.Description;
        Durable = job.Durable;
        RequestsRecovery = job.RequestsRecovery;
    }

    private static string GetId(string scheduler, string group, string name) =>
        $"J{scheduler}/{group}/{name}";

    [JsonProperty]
    public string Name { get; set; } = string.Empty;

    [JsonProperty]
    public string Group { get; set; } = string.Empty;

    [JsonProperty] 
    public string Id { get; set; } = string.Empty;

    [JsonProperty]
    public string Scheduler { get; set; } = string.Empty;

    [JsonProperty]
    public string? Description { get; set; }
    
    [JsonProperty]
    public bool Durable { get; set; }
    
    [JsonProperty]
    public bool RequestsRecovery { get; set; }

    [JsonIgnore]
    public JobKey JobKey =>
        new(Name, Group);
    

    [JsonIgnore]
    public IJobDetail? Item
    {
        get => (IJobDetail?)Deserialize(QuartzData);
        set => QuartzData = Serialize(value);
    }
}

internal static class JobKeyExtensions
{
    public static string GetDatabaseId(this JobKey? jobKey, string schedulerName) => 
        $"J{schedulerName}/{jobKey?.Group ?? string.Empty}/{jobKey?.Name ?? string.Empty}";
}

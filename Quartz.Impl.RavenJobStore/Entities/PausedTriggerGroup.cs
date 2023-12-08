using Newtonsoft.Json;

namespace Quartz.Impl.RavenJobStore.Entities;

internal class PausedTriggerGroup
{
    internal PausedTriggerGroup(string scheduler, string group)
    {
        Scheduler = scheduler;
        GroupName = group;
        Id = GetId(scheduler, group);
    }

    public static string GetId(string scheduler, string group) =>
        $"T{scheduler}#{group}";
    
    [JsonProperty]
    public string Id { get; set; }
    
    [JsonProperty]
    public string Scheduler { get; set; }
    
    [JsonProperty]
    public string GroupName { get; set; }
}
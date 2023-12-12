using Newtonsoft.Json;

namespace Domla.Quartz.Raven.Entities;

internal class PausedJobGroup
{
    internal PausedJobGroup(string scheduler, string group)
    {
        Scheduler = scheduler;
        GroupName = group;
        Id = GetId(scheduler, group);
    }
    
    public static string GetId(string scheduler, string group) =>
        $"J{scheduler}#{group}";
    
    [JsonProperty]
    public string Id { get; set; }
    
    [JsonProperty]
    public string Scheduler { get; set; }
    
    [JsonProperty]
    public string GroupName { get; set; }
}
using Newtonsoft.Json;

namespace Domla.Quartz.Raven.Entities;

internal class BlockedJob
{
    internal BlockedJob(string schedulerInstanceName, string jobId)
    {
        Scheduler = schedulerInstanceName;
        JobId = jobId;
        Id = GetId(Scheduler, JobId);
    }
    
    [JsonProperty] 
    public string Id { get; set; }

    [JsonProperty]
    public string Scheduler { get; init; }

    [JsonProperty]
    public string JobId { get; init; }

    public static string GetId(string scheduler, string jobId) =>
        $"{scheduler}/{jobId}";
}
using Newtonsoft.Json;

namespace Quartz.Impl.RavenJobStore.Entities;

public enum SchedulerState
{
    Unknown,
    Started,
    Paused,
    Resumed,
    Shutdown
}

public class Scheduler
{
    // ReSharper disable once UnusedAutoPropertyAccessor.Global
    [JsonProperty]
    public string InstanceName { get; set; } = null!;

    [JsonProperty]
    public DateTimeOffset LastCheckinTime { get; set; } = DateTimeOffset.MinValue;

    [JsonProperty]
    public DateTimeOffset CheckinInterval { get; set; } = DateTimeOffset.MinValue;

    public SchedulerState State { get; set; } = SchedulerState.Unknown;

    [JsonProperty]
    public Dictionary<string, ICalendar> Calendars { get; set; } = new();

    [JsonProperty]
    public HashSet<string> BlockedJobs { get; set; } = new();
}

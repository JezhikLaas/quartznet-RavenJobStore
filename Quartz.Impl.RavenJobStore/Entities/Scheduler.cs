using Newtonsoft.Json;

namespace Domla.Quartz.Raven.Entities;

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
}

using Newtonsoft.Json;

namespace Quartz.Impl.RavenJobStore;

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
    public string InstanceName { get; set; } = null!;

    public DateTimeOffset LastCheckinTime { get; set; } = DateTimeOffset.MinValue;

    public DateTimeOffset CheckinInterval { get; set; } = DateTimeOffset.MinValue;

    public SchedulerState State { get; set; } = SchedulerState.Unknown;

    public Dictionary<string, ICalendar> Calendars { get; set; } = new();

    public HashSet<string> BlockedJobs { get; set; } = new();
}

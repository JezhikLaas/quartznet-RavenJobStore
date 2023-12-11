using Newtonsoft.Json;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl.RavenJobStore.Entities;

internal class Trigger : SerializeQuartzData
{
    public Trigger(IOperableTrigger? trigger, string schedulerInstanceName)
    {
        if (trigger == null) return;

        Name = trigger.Key.Name;
        Group = trigger.Key.Group;
        Scheduler = schedulerInstanceName;

        Id = GetId(Scheduler, Group, Name);

        Item = trigger;

        // ReSharper disable once ConditionalAccessQualifierIsNonNullableAccordingToAPIContract
        JobName = trigger.JobKey?.Name ?? string.Empty;
        JobGroup = trigger.JobKey?.Group ?? string.Empty;
        JobId = trigger.JobKey != null ? trigger.JobKey.GetDatabaseId(Scheduler) : string.Empty;
        CalendarId = string.IsNullOrEmpty(trigger.CalendarName) == false
            ? Calendar.GetId(Scheduler, trigger.CalendarName)
            : string.Empty;

        State = InternalTriggerState.Waiting;
        Description = trigger.Description;
        CalendarName = trigger.CalendarName;
        JobDataMap = trigger.JobDataMap.WrappedMap;
        MisfireInstruction = trigger.MisfireInstruction;
        Priority = trigger.Priority;
        FireInstanceIdInternal = trigger.FireInstanceId;
        NextFireTimeUtc = trigger.GetNextFireTimeUtc();
    }

    public static string GetId(string scheduler, string group, string name) =>
        $"T{scheduler}/{group}/{name}";
    
    public TriggerKey TriggerKey => 
        new(Name, Group);

    [JsonProperty(PropertyName = nameof(FireInstanceId))]
    private string? FireInstanceIdInternal { get; set; }

    [JsonProperty]
    public string Name { get; set; } = string.Empty;
    
    [JsonProperty]
    public string Group { get; set; } = string.Empty;

    [JsonProperty]
    public string Id { get; set; } = string.Empty;

    [JsonProperty]
    public string JobName { get; set; } = string.Empty;
    
    [JsonProperty]
    public string JobGroup { get; set; } = string.Empty;
    
    [JsonProperty]
    public string JobId { get; set; } = string.Empty;
    
    [JsonProperty]
    public string CalendarId { get; set; } = string.Empty;
    
    [JsonProperty]
    public string Scheduler { get; set; } = string.Empty;

    public InternalTriggerState State { get; set; }
    
    [JsonProperty]
    public string? Description { get; set; }
    
    [JsonProperty]
    public string? CalendarName { get; set; }
    
    [JsonProperty]
    public IDictionary<string, object>? JobDataMap { get; set; }

    [JsonIgnore]
    public string? FireInstanceId
    {
        get => FireInstanceIdInternal;
        set
        {
            var operable = Item;
            if (operable != null)
            {
                operable.FireInstanceId = value ?? string.Empty;
                FireInstanceIdInternal = value;
                QuartzData = Serialize(operable);
            }
        }
    }
    
    [JsonProperty]
    public int MisfireInstruction { get; set; }
    
    [JsonProperty]
    public DateTimeOffset? NextFireTimeUtc { get; set; }
    
    [JsonProperty]
    public DateTimeOffset? PreviousFireTimeUtc { get; set; }

    [JsonProperty]
    public int Priority { get; set; }

    [JsonIgnore]
    public IOperableTrigger? Item
    {
        get
        {
            var result =  (IOperableTrigger?)Deserialize(QuartzData);
            result?.SetNextFireTimeUtc(NextFireTimeUtc);
            result?.SetPreviousFireTimeUtc(PreviousFireTimeUtc);
            
            return result;
        }
        set
        {
            NextFireTimeUtc = value?.GetNextFireTimeUtc();
            PreviousFireTimeUtc = value?.GetPreviousFireTimeUtc();
            
            QuartzData = Serialize(value);
        }
    }
}

internal static class TriggerKeyExtensions
{
    public static string GetDatabaseId(this TriggerKey triggerKey, string schedulerName) => 
        $"T{schedulerName}/{triggerKey.Group}/{triggerKey.Name}";
}

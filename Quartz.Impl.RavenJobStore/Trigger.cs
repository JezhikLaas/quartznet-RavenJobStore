using System.Text.Json;
using Newtonsoft.Json;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;
using JsonException = System.Text.Json.JsonException;

namespace Quartz.Impl.RavenJobStore;

internal class Trigger
{
    public Trigger(IOperableTrigger? quartzTrigger, string schedulerInstanceName)
    {
        if (quartzTrigger == null) return;

        Name = quartzTrigger.Key.Name;
        Group = quartzTrigger.Key.Group;

        // ReSharper disable once ConditionalAccessQualifierIsNonNullableAccordingToAPIContract
        JobName = quartzTrigger.JobKey?.Name ?? string.Empty;
        JobGroup = quartzTrigger.JobKey?.Group ?? string.Empty;
        JobKey = quartzTrigger.JobKey != null ? $"{JobName}/{quartzTrigger.JobKey.Group}" : string.Empty;
        Scheduler = schedulerInstanceName;

        State = InternalTriggerState.Waiting;
        Description = quartzTrigger.Description;
        CalendarName = quartzTrigger.CalendarName;
        JobDataMap = quartzTrigger.JobDataMap.WrappedMap;
        FinalFireTimeUtc = quartzTrigger.FinalFireTimeUtc;
        MisfireInstruction = quartzTrigger.MisfireInstruction;
        Priority = quartzTrigger.Priority;
        HasMillisecondPrecision = quartzTrigger.HasMillisecondPrecision;
        FireInstanceId = quartzTrigger.FireInstanceId;
        EndTimeUtc = quartzTrigger.EndTimeUtc;
        StartTimeUtc = quartzTrigger.StartTimeUtc;
        NextFireTimeUtc = quartzTrigger.GetNextFireTimeUtc();
        PreviousFireTimeUtc = quartzTrigger.GetPreviousFireTimeUtc();

        if (NextFireTimeUtc != null) NextFireTimeTicks = NextFireTimeUtc.Value.UtcTicks;

        // Init trigger specific properties according to type of newTrigger. 
        // If an option doesn't apply to the type of trigger it will stay null by default.

        switch (quartzTrigger)
        {
            case CronTriggerImpl cronTriggerImpl:
                CronTriggerOptions = new CronOptions
                {
                    CronExpression = cronTriggerImpl.CronExpressionString,
                    TimeZoneId = cronTriggerImpl.TimeZone.Id
                };
                return;
            case SimpleTriggerImpl simpTriggerImpl:
                SimpleTriggerOptions = new SimpleOptions
                {
                    RepeatCount = simpTriggerImpl.RepeatCount,
                    RepeatInterval = simpTriggerImpl.RepeatInterval
                };
                return;
            case CalendarIntervalTriggerImpl calTriggerImpl:
                CalendarIntervalTriggerOptions = new CalendarOptions
                {
                    RepeatIntervalUnit = calTriggerImpl.RepeatIntervalUnit,
                    RepeatInterval = calTriggerImpl.RepeatInterval,
                    TimesTriggered = calTriggerImpl.TimesTriggered,
                    TimeZoneId = calTriggerImpl.TimeZone.Id,
                    PreserveHourOfDayAcrossDaylightSavings = calTriggerImpl.PreserveHourOfDayAcrossDaylightSavings,
                    SkipDayIfHourDoesNotExist = calTriggerImpl.SkipDayIfHourDoesNotExist
                };
                return;
            case DailyTimeIntervalTriggerImpl dayTriggerImpl:
                DailyTimeIntervalTriggerOptions = new DailyTimeOptions
                {
                    RepeatCount = dayTriggerImpl.RepeatCount,
                    RepeatIntervalUnit = dayTriggerImpl.RepeatIntervalUnit,
                    RepeatInterval = dayTriggerImpl.RepeatInterval,
                    StartTimeOfDay = dayTriggerImpl.StartTimeOfDay,
                    EndTimeOfDay = dayTriggerImpl.EndTimeOfDay,
                    DaysOfWeek = dayTriggerImpl.DaysOfWeek,
                    TimesTriggered = dayTriggerImpl.TimesTriggered,
                    TimeZoneId = dayTriggerImpl.TimeZone.Id
                };
                break;
        }
    }

    public TriggerKey TriggerKey => 
        new(Name, Group);

    [JsonProperty]
    public string Name { get; set; } = string.Empty;
    
    [JsonProperty]
    public string Group { get; set; } = string.Empty;

    [JsonProperty]
    public string Key => $"{Name}/{Group}";

    [JsonProperty]
    public string JobName { get; set; } = string.Empty;
    
    [JsonProperty]
    public string JobGroup { get; set; } = string.Empty;
    
    [JsonProperty]
    public string JobKey { get; set; } = string.Empty;
    
    [JsonProperty]
    public string Scheduler { get; set; } = string.Empty;

    public InternalTriggerState State { get; set; }
    
    [JsonProperty]
    public string? Description { get; set; }
    
    [JsonProperty]
    public string? CalendarName { get; set; }
    
    [JsonProperty]
    public IDictionary<string, object>? JobDataMap { get; set; }

    public string FireInstanceId { get; set; } = string.Empty;
    
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
    public int MisfireInstruction { get; set; }
    
    // ReSharper disable once UnusedAutoPropertyAccessor.Global
    public DateTimeOffset? FinalFireTimeUtc { get; set; }
    
    [JsonProperty]
    public DateTimeOffset? EndTimeUtc { get; set; }
    
    [JsonProperty]
    public DateTimeOffset StartTimeUtc { get; set; }
    
    [JsonProperty]
    public DateTimeOffset? NextFireTimeUtc { get; set; }

    [JsonProperty]
    public long NextFireTimeTicks { get; set; }

    [JsonProperty]
    public DateTimeOffset? PreviousFireTimeUtc { get; set; }

    [JsonProperty]
    public int Priority { get; set; }
    
    [JsonProperty]
    public bool HasMillisecondPrecision { get; set; }

    [JsonProperty]
    public CronOptions? CronTriggerOptions { get; set; }
    
    [JsonProperty]
    public SimpleOptions? SimpleTriggerOptions { get; set; }
    
    [JsonProperty]
    public CalendarOptions? CalendarIntervalTriggerOptions { get; set; }
    
    [JsonProperty]
    public DailyTimeOptions? DailyTimeIntervalTriggerOptions { get; set; }

    public void UpdateFireTimes(ITrigger trig)
    {
        NextFireTimeUtc = trig.GetNextFireTimeUtc();
        PreviousFireTimeUtc = trig.GetPreviousFireTimeUtc();
        if (NextFireTimeUtc != null) NextFireTimeTicks = NextFireTimeUtc.Value.UtcTicks;
    }

/// <summary>
    ///     Converts this <see cref="Trigger"/> back into an <see cref="IOperableTrigger"/>.
    /// </summary>
    /// <returns>The built <see cref="IOperableTrigger"/>.</returns>
    public IOperableTrigger Deserialize()
    {
        var triggerBuilder = TriggerBuilder.Create()
            .WithIdentity(Name, Group)
            .WithDescription(Description)
            .ModifiedByCalendar(CalendarName)
            .WithPriority(Priority)
            .StartAt(StartTimeUtc)
            .EndAt(EndTimeUtc)
            .ForJob(new JobKey(JobName, Group))
            .UsingJobData(new JobDataMap(JobDataMap ?? new Dictionary<string, object>()));

        if (CronTriggerOptions != null)
        {
            triggerBuilder = triggerBuilder.WithCronSchedule(CronTriggerOptions.CronExpression.ThrowIfNull(), builder =>
            {
                builder
                    .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(CronTriggerOptions.TimeZoneId.ThrowIfNull()));
            });
        }
        else if (SimpleTriggerOptions != null)
        {
            triggerBuilder = triggerBuilder.WithSimpleSchedule(builder =>
            {
                builder
                    .WithInterval(SimpleTriggerOptions.RepeatInterval)
                    .WithRepeatCount(SimpleTriggerOptions.RepeatCount);
            });
        }
        else if (CalendarIntervalTriggerOptions != null)
        {
            triggerBuilder = triggerBuilder.WithCalendarIntervalSchedule(builder =>
            {
                builder
                    .WithInterval(
                        CalendarIntervalTriggerOptions.RepeatInterval,
                        CalendarIntervalTriggerOptions.RepeatIntervalUnit)
                    .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(CalendarIntervalTriggerOptions.TimeZoneId.ThrowIfNull()))
                    .PreserveHourOfDayAcrossDaylightSavings(
                        CalendarIntervalTriggerOptions
                        .PreserveHourOfDayAcrossDaylightSavings)
                    .SkipDayIfHourDoesNotExist(CalendarIntervalTriggerOptions.SkipDayIfHourDoesNotExist);
            });
        }
        else if (DailyTimeIntervalTriggerOptions != null)
        {
            triggerBuilder = triggerBuilder.WithDailyTimeIntervalSchedule(builder =>
            {
                builder
                    .WithRepeatCount(DailyTimeIntervalTriggerOptions.RepeatCount)
                    .WithInterval(DailyTimeIntervalTriggerOptions.RepeatInterval,
                        DailyTimeIntervalTriggerOptions.RepeatIntervalUnit)
                    .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(DailyTimeIntervalTriggerOptions.TimeZoneId.ThrowIfNull()))
                    .EndingDailyAt(DailyTimeIntervalTriggerOptions.EndTimeOfDay.ThrowIfNull())
                    .StartingDailyAt(DailyTimeIntervalTriggerOptions.StartTimeOfDay.ThrowIfNull())
                    .OnDaysOfTheWeek(DailyTimeIntervalTriggerOptions.DaysOfWeek.ThrowIfNull());
            });
        }

        var trigger = triggerBuilder.Build();

        var returnTrigger = (IOperableTrigger) trigger;
        returnTrigger.SetNextFireTimeUtc(NextFireTimeUtc);
        returnTrigger.SetPreviousFireTimeUtc(PreviousFireTimeUtc);
        returnTrigger.FireInstanceId = FireInstanceId;

        return returnTrigger;
    }

    public class CronOptions
    {
        [JsonProperty]
        public string? CronExpression { get; init; }

        [JsonProperty]
        public string? TimeZoneId { get; init; }
    }

    public class SimpleOptions
    {
        [JsonProperty]
        public int RepeatCount { get; init; }

        [JsonProperty]
        public TimeSpan RepeatInterval { get; init; }
    }

    public class CalendarOptions
    {
        [JsonProperty]
        public IntervalUnit RepeatIntervalUnit { get; init; }
        
        [JsonProperty]
        public int RepeatInterval { get; init; }
        
        [JsonProperty]
        public int TimesTriggered { get; init; }
        
        [JsonProperty]
        public string? TimeZoneId { get; init; }
        
        [JsonProperty]
        public bool PreserveHourOfDayAcrossDaylightSavings { get; init; }
        
        [JsonProperty]
        public bool SkipDayIfHourDoesNotExist { get; init; }
    }

    public class DailyTimeOptions
    {
        [JsonProperty]
        public int RepeatCount { get; init; }

        [JsonProperty]
        public IntervalUnit RepeatIntervalUnit { get; init; }

        [JsonProperty]
        public int RepeatInterval { get; init; }

        [JsonProperty]
        [System.Text.Json.Serialization.JsonConverter(typeof(TimeOfDayConverter))]
        public TimeOfDay? StartTimeOfDay { get; init; }

        [JsonProperty]
        [System.Text.Json.Serialization.JsonConverter(typeof(TimeOfDayConverter))]
        public TimeOfDay? EndTimeOfDay { get; init; }

        [JsonProperty]
        public IReadOnlyCollection<DayOfWeek>? DaysOfWeek { get; init; }

        [JsonProperty]
        public int TimesTriggered { get; set; }

        [JsonProperty]
        public string? TimeZoneId { get; init; }
    }
}

internal class TimeOfDayConverter : System.Text.Json.Serialization.JsonConverter<TimeOfDay>
{
    public override TimeOfDay Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var hour = 0;
        var minute = 0;
        var second = 0;

        while (reader.Read())
        {
            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                return new TimeOfDay(hour, minute, second);
            }

            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var name = reader.GetString();
                reader.Read();

                switch (name)
                {
                    case nameof(TimeOfDay.Hour):
                        hour = reader.GetInt32();
                        break;
                    case nameof(TimeOfDay.Minute):
                        minute = reader.GetInt32();
                        break;
                    case nameof(TimeOfDay.Second):
                        second = reader.GetInt32();
                        break;
                }
            }
        }

        throw new JsonException("Unable to convert data to instance of TimeOfDay");
    }

    public override void Write(Utf8JsonWriter writer, TimeOfDay value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}

internal static class TriggerKeyExtensions
{
    public static string GetDatabaseId(this TriggerKey triggerKey) => 
        $"{triggerKey.Name}/{triggerKey.Group}";
}

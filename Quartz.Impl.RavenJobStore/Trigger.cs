using System.Text.Json;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

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

    public string Name { get; set; } = string.Empty;
    
    public string Group { get; set; } = string.Empty;
    
    public string Key => $"{Name}/{Group}";

    public string JobName { get; set; } = string.Empty;
    
    public string JobGroup { get; set; } = string.Empty;
    
    public string JobKey { get; set; } = string.Empty;
    
    public string Scheduler { get; set; } = string.Empty;

    public InternalTriggerState State { get; set; }
    
    public string? Description { get; set; }
    
    public string? CalendarName { get; set; }
    
    public IDictionary<string, object>? JobDataMap { get; set; }

    public string FireInstanceId { get; set; } = string.Empty;
    
    public int MisfireInstruction { get; set; }
    
    public DateTimeOffset? FinalFireTimeUtc { get; set; }
    
    public DateTimeOffset? EndTimeUtc { get; set; }
    
    public DateTimeOffset StartTimeUtc { get; set; }

    
    public DateTimeOffset? NextFireTimeUtc { get; set; }

    public long NextFireTimeTicks { get; set; }

    public DateTimeOffset? PreviousFireTimeUtc { get; set; }

    public int Priority { get; set; }
    
    public bool HasMillisecondPrecision { get; set; }

    public CronOptions? CronTriggerOptions { get; set; }
    
    public SimpleOptions? SimpleTriggerOptions { get; set; }
    
    public CalendarOptions? CalendarIntervalTriggerOptions { get; set; }
    
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
        public string? CronExpression { get; set; }
        public string? TimeZoneId { get; set; }
    }

    public class SimpleOptions
    {
        public int RepeatCount { get; set; }
        public TimeSpan RepeatInterval { get; set; }
    }

    public class CalendarOptions
    {
        public IntervalUnit RepeatIntervalUnit { get; set; }
        public int RepeatInterval { get; set; }
        public int TimesTriggered { get; set; }
        public string? TimeZoneId { get; set; }
        public bool PreserveHourOfDayAcrossDaylightSavings { get; set; }
        public bool SkipDayIfHourDoesNotExist { get; set; }
    }

    public class DailyTimeOptions
    {
        public int RepeatCount { get; set; }

        public IntervalUnit RepeatIntervalUnit { get; set; }

        public int RepeatInterval { get; set; }

        [System.Text.Json.Serialization.JsonConverter(typeof(TimeOfDayConverter))]
        public TimeOfDay? StartTimeOfDay { get; set; }

        [System.Text.Json.Serialization.JsonConverter(typeof(TimeOfDayConverter))]
        public TimeOfDay? EndTimeOfDay { get; set; }

        public IReadOnlyCollection<DayOfWeek>? DaysOfWeek { get; set; }

        public int TimesTriggered { get; set; }

        public string? TimeZoneId { get; set; }
    }
}

internal class TimeOfDayConverter : System.Text.Json.Serialization.JsonConverter<TimeOfDay>
{
    public override TimeOfDay? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var hour = 0;
        var minute = 0;
        var second = 0;

        while (reader.Read())
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.EndObject:
                    return new TimeOfDay(hour, minute, second);
               
                case JsonTokenType.PropertyName:
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

internal class TriggerComparator : IComparer<Trigger>
{
    public int Compare(Trigger? triggerOne, Trigger? triggerTwo)
    {
        var timeOne = triggerOne?.NextFireTimeUtc;
        var timeTwo = triggerTwo?.NextFireTimeUtc;

        if (timeOne != null || timeTwo != null)
        {
            if (timeOne == null) return 1;

            if (timeTwo == null) return -1;

            if (timeOne < timeTwo) return -1;

            if (timeOne > timeTwo) return 1;
        }

        var comparison = triggerTwo?.Priority ?? 0 - triggerOne?.Priority ?? 0;
        return comparison != 0 ? comparison : 0;
    }
}

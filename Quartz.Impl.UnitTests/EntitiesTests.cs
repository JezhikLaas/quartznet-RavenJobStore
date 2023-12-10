using FluentAssertions;
using Newtonsoft.Json;
using Quartz.Impl.Calendar;
using Quartz.Impl.Triggers;
using Quartz.Impl.UnitTests.Helpers;
using Quartz.Spi;
using Xunit.Abstractions;

namespace Quartz.Impl.UnitTests;

using RavenJobStore;
using RavenJobStore.Entities;

public class EntitiesTests : TestBase
{
    private RavenJobStore Target { get; }
    
    private ITestOutputHelper Output { get; }

    public EntitiesTests(ITestOutputHelper output)
    {
        var store = CreateStore();
        
        Output = output;
        Target = new RavenJobStore(store)
        {
            Logger = Output.BuildLoggerFor<RavenJobStore>()
        };
    }

    [Theory(DisplayName = "If a calendar is stored and loaded Then its item is correctly deserialized")]
    [MemberData(nameof(GetCalendars))]
    public async Task If_a_calendar_is_stored_and_loaded_Then_its_item_is_correctly_deserialized(ICalendar item)
    {
        using var store = CreateStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Calendar(item, "test", Target.InstanceName));
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var calendar = await session.LoadAsync<Calendar>
            (
                Calendar.GetId(Target.InstanceName, "test")
            );

            calendar.Item.Should().BeEquivalentTo(item);
        }
    }

    [Theory(DisplayName = "If a trigger is stored and loaded Then its item is correctly deserialized")]
    [MemberData(nameof(GetTriggers))]
    public async Task If_a_trigger_is_stored_and_loaded_Then_its_item_is_correctly_deserialized(IOperableTrigger item)
    {
        using var store = CreateStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Trigger(item, Target.InstanceName));
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var trigger = await session.LoadAsync<Trigger>
            (
                Trigger.GetId(Target.InstanceName, item.Key.Group, item.Key.Name)
            );

            trigger.Item.Should().BeEquivalentTo(item);
        }
    }

    [Fact(DisplayName = "If types with converters are null Then json conversion succeeds")]
    public void If_types_with_converters_are_null_Then_json_conversion_succeeds()
    {
        var settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects,
            Converters = new List<JsonConverter>
            {
                new SerializeQuartzData.TimeOfDayConverter(),
                new SerializeQuartzData.TimeZoneInfoConverter()
            }
        };
        
        var source = new TestPropertyConverters();
        
        var target = JsonConvert.DeserializeObject<TestPropertyConverters>
        (
            JsonConvert.SerializeObject(source, settings),
            settings
        );

        target.Should().BeEquivalentTo(source);
    }

    [Fact(DisplayName = "If types with converters are not null Then json conversion succeeds")]
    public void If_types_with_converters_are_not_null_Then_json_conversion_succeeds()
    {
        var settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects,
            Converters = new List<JsonConverter>
            {
                new SerializeQuartzData.TimeOfDayConverter(),
                new SerializeQuartzData.TimeZoneInfoConverter()
            }
        };
        
        var source = new TestPropertyConverters
        {
            Value1 = new TimeOfDay(10, 30, 15),
            Value2 = TimeZoneInfo.FindSystemTimeZoneById("Europe/Berlin")
        };
        
        var target = JsonConvert.DeserializeObject<TestPropertyConverters>
        (
            JsonConvert.SerializeObject(source, settings),
            settings
        );

        target.Should().BeEquivalentTo(source);
    }

    public static IEnumerable<object[]> GetCalendars()
    {
        yield return new object[]
        {
            new DailyCalendar(DateTime.UtcNow, DateTime.UtcNow.AddHours(1))
        };
        yield return new object[]
        {
            new DailyCalendar
            (
                new WeeklyCalendar { DaysExcluded = new []{ false, false, false, true, false, false, false, false }},
                DateTime.UtcNow,
                DateTime.UtcNow.AddHours(1)
            )
        };
        yield return new object[]
        {
            new WeeklyCalendar { DaysExcluded = new[] { false, false, false, true, false, false, true, false }, TimeZone = TimeZoneInfo.Local }
        };

        yield return new object[]
        {
            new CronCalendar("0 0 4 * * ?")
        };

        yield return new object[]
        {
            new HolidayCalendar()
        };
    }

    public static IEnumerable<object[]> GetTriggers()
    {
        yield return new object[]
        {
            new SimpleTriggerImpl("T", "X", DateTimeOffset.UtcNow)
            {
                JobName = "Y"
            }
        };

        yield return new object[]
        {
            new CronTriggerImpl("T", "X", "0 0 4 * * ?")
            {
                JobName = "Y"
            }
        };

        yield return new object[]
        {
            new CalendarIntervalTriggerImpl("T", "X", DateTimeOffset.UtcNow, null, IntervalUnit.Day, 1)
            {
                JobName = "Y"
            }
        };

        yield return new object[]
        {
            new DailyTimeIntervalTriggerImpl("T", "X", DateTimeOffset.UtcNow, null, TimeOfDay.HourAndMinuteOfDay(10, 0), TimeOfDay.HourAndMinuteOfDay(11, 0), IntervalUnit.Hour, 2)
            {
                JobName = "Y"
            }
        };
    }

    public class TestPropertyConverters
    {
        [JsonProperty]
        public TimeOfDay? Value1 { get; set; }
        
        [JsonProperty]
        public TimeZoneInfo? Value2 { get; set; }
    }
}
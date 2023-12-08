using Newtonsoft.Json;

namespace Quartz.Impl.RavenJobStore.Entities;

internal class Calendar : SerializeQuartzData
{
    public Calendar(ICalendar item, string name, string schedulerName)
    {
        Item = item;
        Name = name;
        Scheduler = schedulerName;
        Id = GetId(Scheduler, Name);
    }

    [JsonProperty]
    public string Id { get; init; }

    [JsonIgnore]
    public ICalendar? Item
    {
        get => (ICalendar?)Deserialize(QuartzData);
        private init => QuartzData = Serialize(value);
    }

    [JsonProperty]
    public string Name { get; }

    [JsonProperty]
    public string Scheduler { get; init; }

    public static string GetId(string schedulerName, string name) =>
        $"{schedulerName}/{name}";
}
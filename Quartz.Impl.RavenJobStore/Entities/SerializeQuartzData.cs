using Newtonsoft.Json;

namespace Quartz.Impl.RavenJobStore.Entities;

internal abstract class SerializeQuartzData
{
    private static readonly JsonSerializerSettings Settings = new()
    {
        TypeNameHandling = TypeNameHandling.Objects,
        Converters = new List<JsonConverter> { new TimeOfDayConverter(), new TimeZoneInfoConverter() }
    };
    
    [JsonProperty]
    protected string? QuartzData { get; set; }

    protected static string? Serialize(object? quartzInstance) =>
        quartzInstance != null
            ? JsonConvert.SerializeObject(quartzInstance, Settings)
            : null;

    protected static object? Deserialize(string? serialized) =>
        serialized != null
            ? JsonConvert.DeserializeObject(serialized, Settings)
            : null;

    internal class TimeOfDayConverter : JsonConverter<TimeOfDay>
    {
        public override void WriteJson(JsonWriter writer, TimeOfDay? value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartObject();
            writer.WritePropertyName(nameof(TimeOfDay.Hour));
            writer.WriteValue(value.Hour);
            writer.WritePropertyName(nameof(TimeOfDay.Minute));
            writer.WriteValue(value.Minute);
            writer.WritePropertyName(nameof(TimeOfDay.Second));
            writer.WriteValue(value.Second);
            writer.WriteEndObject();
        }

        public override TimeOfDay? ReadJson(JsonReader reader,
            Type objectType,
            TimeOfDay? existingValue,
            bool hasExistingValue,
            JsonSerializer serializer)
        {
            var hour = 0;
            var minute = 0;
            var second = 0;

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.EndObject) break;
                
                // ReSharper disable once SwitchStatementMissingSomeEnumCasesNoDefault
                switch (reader.TokenType)
                {
                    case JsonToken.Null:
                        return null;
                    case JsonToken.PropertyName when reader.Value == null:
                        continue;
                    case JsonToken.PropertyName:
                        switch (reader.Value.ToString())
                        {
                            case nameof(TimeOfDay.Hour):
                                hour = reader.ReadAsInt32() ?? 0;
                                break;
                            case nameof(TimeOfDay.Minute):
                                minute = reader.ReadAsInt32() ?? 0;
                                break;
                            case nameof(TimeOfDay.Second):
                                second = reader.ReadAsInt32() ?? 0;
                                break;
                        }

                        break;
                }
            }

            return new TimeOfDay(hour, minute, second);
        }
    }

    internal class TimeZoneInfoConverter : JsonConverter<TimeZoneInfo>
    {
        public override void WriteJson(JsonWriter writer, TimeZoneInfo? value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteValue(value.Id);
        }

        public override TimeZoneInfo? ReadJson(JsonReader reader,
            Type objectType,
            TimeZoneInfo? existingValue,
            bool hasExistingValue,
            JsonSerializer serializer)
        {
            if (reader.Value == null) return existingValue;

            var id = reader.Value.ToString();
            return string.IsNullOrWhiteSpace(id) ? null : TimeZoneInfo.FindSystemTimeZoneById(id);
        }
    }
}

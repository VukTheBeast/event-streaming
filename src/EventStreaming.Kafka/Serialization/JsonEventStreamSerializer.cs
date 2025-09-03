using System;
using System.Text;
using System.Text.Json;
using EventStreaming.Kafka.Serialization;

namespace EventStreaming.Kafka.Serialization
{
    public class JsonEventStreamSerializer<T> : IEventStreamSerializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public JsonEventStreamSerializer(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                PropertyNameCaseInsensitive = true,
                WriteIndented = false
            };
        }

        public byte[] Serialize(T data)
        {
            if (data == null) return Array.Empty<byte>();
            
            var json = JsonSerializer.Serialize(data, _options);
            return Encoding.UTF8.GetBytes(json);
        }

        public T? Deserialize(byte[] data)
        {
            if (data == null || data.Length == 0) return default;
            
            var json = Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize<T>(json, _options);
        }
    }
}

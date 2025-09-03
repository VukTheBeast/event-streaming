using System;
using System.Collections.Generic;

namespace EventStreaming.Core.Models
{
    public class EventStreamMessage<T>
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Topic { get; set; } = string.Empty;
        public string? Key { get; set; }
        public T Data { get; set; } = default!;
        public Dictionary<string, string> Headers { get; set; } = new();
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        public string? CorrelationId { get; set; }
        public string? SourceSystem { get; set; }
        public int Version { get; set; } = 1;
    }
}
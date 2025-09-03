using EventStreaming.Core.Configuration;

namespace EventStreaming.Kafka.Configuration
{
    public class KafkaProducerOptions : ProducerOptions
    {
        public string? Acks { get; set; } = "all";
        public int? LingerMs { get; set; } = 5;
        public string? CompressionType { get; set; } = "snappy";
        public int? RequestTimeoutMs { get; set; } = 30000;
        public int? DeliveryTimeoutMs { get; set; } = 120000;
    }

    public class KafkaConsumerOptions : ConsumerOptions
    {
        public string? AutoOffsetReset { get; set; } = "earliest";
        public bool? EnablePartitionEof { get; set; } = false;
        public int? FetchMinBytes { get; set; } = 1;
        public int? FetchMaxWaitMs { get; set; } = 500;
    }

    public class KafkaListenerOptions : ListenerOptions
    {
        public string? AutoOffsetReset { get; set; } = "earliest";
        public bool? EnablePartitionEof { get; set; } = false;
        public int? FetchMinBytes { get; set; } = 1;
        public int? FetchMaxWaitMs { get; set; } = 500;
    }
}

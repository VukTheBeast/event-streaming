using Confluent.Kafka;
using EventStreaming.Core.Abstractions;
using EventStreaming.Core.Models;
using EventStreaming.Kafka.Configuration;
using EventStreaming.Kafka.Serialization;
using Microsoft.Extensions.Logging;

namespace EventStreaming.Kafka
{
    public class KafkaEventStreamProducer<T> : IEventStreamProducer<T>
    {
        private readonly IProducer<string, byte[]> _producer;
        private readonly IEventStreamSerializer<T> _serializer;
        private readonly ILogger<KafkaEventStreamProducer<T>> _logger;
        private readonly KafkaProducerOptions _options;
        private bool _disposed;

        public KafkaEventStreamProducer(
            KafkaProducerOptions options,
            IEventStreamSerializer<T>? serializer = null,
            ILogger<KafkaEventStreamProducer<T>>? logger = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();
            
            _serializer = serializer ?? new JsonEventStreamSerializer<T>();
            _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<KafkaEventStreamProducer<T>>.Instance;

            var config = CreateProducerConfig();
            _producer = new ProducerBuilder<string, byte[]>(config).Build();
        }

        public async Task<EventStreamResult> PublishAsync(string topic, T data, string? key = null, CancellationToken cancellationToken = default)
        {
            var message = new EventStreamMessage<T>
            {
                Topic = topic,
                Key = key,
                Data = data
            };

            return await PublishAsync(message, cancellationToken);
        }

        public async Task<EventStreamResult> PublishAsync(EventStreamMessage<T> message, CancellationToken cancellationToken = default)
        {
            try
            {
                ThrowIfDisposed();

                var serializedData = _serializer.Serialize(message.Data);
                var kafkaMessage = new Message<string, byte[]>
                {
                    Key = message.Key,
                    Value = serializedData,
                    Headers = CreateHeaders(message)
                };

                _logger.LogDebug("Publishing message {MessageId} to topic {Topic}", message.Id, message.Topic);

                var deliveryResult = await _producer.ProduceAsync(message.Topic, kafkaMessage, cancellationToken);

                _logger.LogDebug("Message {MessageId} published successfully to {Topic} at offset {Offset}", 
                    message.Id, deliveryResult.Topic, deliveryResult.Offset);

                return EventStreamResult.Success(message.Id, deliveryResult.Offset);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish message {MessageId} to topic {Topic}", message.Id, message.Topic);
                return EventStreamResult.Failure(ex);
            }
        }

        public async Task<IEnumerable<EventStreamResult>> PublishBatchAsync(string topic, IEnumerable<T> data, CancellationToken cancellationToken = default)
        {
            var messages = data.Select(d => new EventStreamMessage<T>
            {
                Topic = topic,
                Data = d
            });

            return await PublishBatchAsync(messages, cancellationToken);
        }

        public async Task<IEnumerable<EventStreamResult>> PublishBatchAsync(IEnumerable<EventStreamMessage<T>> messages, CancellationToken cancellationToken = default)
        {
            var results = new List<EventStreamResult>();

            foreach (var message in messages)
            {
                var result = await PublishAsync(message, cancellationToken);
                results.Add(result);

                if (!result.IsSuccess && !_options.ContinueOnError)
                    break;
            }

            return results;
        }

        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            await Task.Run(() => _producer.Flush(_options.FlushTimeout), cancellationToken);
        }

        private ProducerConfig CreateProducerConfig()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _options.ConnectionString,
                EnableIdempotence = _options.EnableIdempotence,
                BatchSize = _options.BatchSize,
                RequestTimeoutMs = _options.RequestTimeoutMs,
                DeliveryTimeoutMs = _options.DeliveryTimeoutMs,
                Acks = _options.Acks switch
                {
                    "0" => Acks.None,
                    "1" => Acks.Leader,
                    "all" or "-1" => Acks.All,
                    _ => Acks.All
                }
            };

            if (_options.LingerMs.HasValue)
                config.LingerMs = _options.LingerMs.Value;

            if (!string.IsNullOrEmpty(_options.CompressionType))
                config.CompressionType = Enum.Parse<CompressionType>(_options.CompressionType, true);

            // Add custom properties
            foreach (var prop in _options.Properties)
                config.Set(prop.Key, prop.Value);

            return config;
        }

        private static Headers CreateHeaders(EventStreamMessage<T> message)
        {
            var headers = new Headers
            {
                { "message-id", System.Text.Encoding.UTF8.GetBytes(message.Id) },
                { "timestamp", System.Text.Encoding.UTF8.GetBytes(message.Timestamp.ToString("O")) },
                { "version", System.Text.Encoding.UTF8.GetBytes(message.Version.ToString()) }
            };

            if (!string.IsNullOrEmpty(message.CorrelationId))
                headers.Add("correlation-id", System.Text.Encoding.UTF8.GetBytes(message.CorrelationId));

            if (!string.IsNullOrEmpty(message.SourceSystem))
                headers.Add("source-system", System.Text.Encoding.UTF8.GetBytes(message.SourceSystem));

            foreach (var header in message.Headers)
                headers.Add(header.Key, System.Text.Encoding.UTF8.GetBytes(header.Value));

            return headers;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(KafkaEventStreamProducer<T>));
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _producer?.Dispose();
                _disposed = true;
            }
        }
    }
}
using Confluent.Kafka;
using EventStreaming.Core.Abstractions;
using EventStreaming.Core.Models;
using EventStreaming.Kafka.Configuration;
using EventStreaming.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventStreaming.Kafka
{
    public class KafkaEventStreamConsumer<T> : IEventStreamConsumer<T>
    {
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly IEventStreamSerializer<T> _serializer;
        private readonly ILogger<KafkaEventStreamConsumer<T>> _logger;
        private readonly KafkaConsumerOptions _options;
        private bool _disposed;

        public KafkaEventStreamConsumer(
            KafkaConsumerOptions options,
            IEventStreamSerializer<T>? serializer = null,
            ILogger<KafkaEventStreamConsumer<T>>? logger = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();

            _serializer = serializer ?? new JsonEventStreamSerializer<T>();
            _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<KafkaEventStreamConsumer<T>>.Instance;

            var config = CreateConsumerConfig();
            _consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        }

        public async Task SubscribeAsync(string topic, CancellationToken cancellationToken = default)
        {
            await SubscribeAsync(new[] { topic }, cancellationToken);
        }

        public async Task SubscribeAsync(IEnumerable<string> topics, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            await Task.Run(() =>
            {
                _consumer.Subscribe(topics);
                _logger.LogInformation("Subscribed to topics: {Topics}", string.Join(", ", topics));
            }, cancellationToken);
        }

        public async Task<EventStreamResult<EventStreamMessage<T>?>> ConsumeAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                ThrowIfDisposed();

                var consumeResult = await Task.Run(() => _consumer.Consume(cancellationToken), cancellationToken);
                
                if (consumeResult?.Message == null)
                    return EventStreamResult<EventStreamMessage<T>?>.Success(null);

                var message = ConvertToEventStreamMessage(consumeResult);
                
                _logger.LogDebug("Consumed message from topic {Topic} at offset {Offset}", 
                    consumeResult.Topic, consumeResult.Offset);

                return EventStreamResult<EventStreamMessage<T>?>.Success(message, 
                    message.Id, consumeResult.Offset);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to consume message");
                return EventStreamResult<EventStreamMessage<T>?>.Failure(ex);
            }
        }

        public async Task<IEnumerable<EventStreamResult<EventStreamMessage<T>>>> ConsumeBatchAsync(
            int maxMessages = 100, 
            TimeSpan? timeout = null, 
            CancellationToken cancellationToken = default)
        {
            var results = new List<EventStreamResult<EventStreamMessage<T>>>();
            var endTime = DateTime.UtcNow.Add(timeout ?? TimeSpan.FromSeconds(30));

            while (results.Count < maxMessages && DateTime.UtcNow < endTime && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = await Task.Run(() => 
                        _consumer.Consume(TimeSpan.FromMilliseconds(100)), cancellationToken);

                    if (consumeResult?.Message == null)
                        continue;

                    var message = ConvertToEventStreamMessage(consumeResult);
                    results.Add(EventStreamResult<EventStreamMessage<T>>.Success(message, 
                        message.Id, consumeResult.Offset));
                }
                catch (Exception ex)
                {
                    results.Add(EventStreamResult<EventStreamMessage<T>>.Failure(ex));
                    if (!_options.ContinueOnError)
                        break;
                }
            }

            return results;
        }

        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            await Task.Run(() => _consumer.Commit(), cancellationToken);
        }

        public async Task CommitAsync(EventStreamMessage<T> message, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            // Note: This would require storing the original ConsumeResult
            // For now, we'll just commit the current position
            await CommitAsync(cancellationToken);
        }

        public void Unsubscribe()
        {
            ThrowIfDisposed();
            _consumer.Unsubscribe();
            _logger.LogInformation("Unsubscribed from all topics");
        }

        private EventStreamMessage<T> ConvertToEventStreamMessage(ConsumeResult<string, byte[]> consumeResult)
        {
            var data = _serializer.Deserialize(consumeResult.Message.Value);
            var headers = ExtractHeaders(consumeResult.Message.Headers);

            return new EventStreamMessage<T>
            {
                Id = headers.TryGetValue("message-id", out var messageId) ? messageId : Guid.NewGuid().ToString(),
                Topic = consumeResult.Topic,
                Key = consumeResult.Message.Key,
                Data = data!,
                Headers = headers,
                Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                CorrelationId = headers.TryGetValue("correlation-id", out var correlationId) ? correlationId : null,
                SourceSystem = headers.TryGetValue("source-system", out var sourceSystem) ? sourceSystem : null,
                Version = headers.TryGetValue("version", out var version) && int.TryParse(version, out var v) ? v : 1
            };
        }

        private static Dictionary<string, string> ExtractHeaders(Headers? headers)
        {
            var result = new Dictionary<string, string>();
            
            if (headers == null) return result;

            foreach (var header in headers)
            {
                try
                {
                    var value = Encoding.UTF8.GetString(header.GetValueBytes());
                    result[header.Key] = value;
                }
                catch
                {
                    // Skip headers that can't be decoded as UTF-8
                }
            }

            return result;
        }

        private ConsumerConfig CreateConsumerConfig()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _options.ConnectionString,
                GroupId = _options.GroupId,
                EnableAutoCommit = _options.EnableAutoCommit,
                AutoCommitIntervalMs = (int)_options.AutoCommitInterval.TotalMilliseconds,
                MaxPollIntervalMs = (int)_options.SessionTimeout.TotalMilliseconds,
                SessionTimeoutMs = (int)_options.SessionTimeout.TotalMilliseconds
            };

            if (!string.IsNullOrEmpty(_options.AutoOffsetReset))
                config.AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_options.AutoOffsetReset, true);

            if (_options.EnablePartitionEof.HasValue)
                config.EnablePartitionEof = _options.EnablePartitionEof.Value;

            if (_options.FetchMinBytes.HasValue)
                config.FetchMinBytes = _options.FetchMinBytes.Value;

            if (_options.FetchMaxWaitMs.HasValue)
                config.FetchMaxWaitMs = _options.FetchMaxWaitMs.Value;

            // Add custom properties
            foreach (var prop in _options.Properties)
                config.Set(prop.Key, prop.Value);

            return config;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(KafkaEventStreamConsumer<T>));
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _consumer?.Close();
                _consumer?.Dispose();
                _disposed = true;
            }
        }
    }
}
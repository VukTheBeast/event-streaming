using EventStreaming.Core.Abstractions;
using EventStreaming.Core.Models;
using EventStreaming.Kafka.Configuration;
using EventStreaming.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace EventStreaming.Kafka
{
    public class KafkaEventListener<T> : IEventListener<T>
    {
        private readonly ConcurrentDictionary<string, EventHandlerAsync<T>> _handlers = new();
        private readonly List<ErrorHandlerAsync> _errorHandlers = new();
        private readonly KafkaEventStreamConsumer<T> _consumer;
        private readonly ILogger<KafkaEventListener<T>> _logger;
        private readonly KafkaListenerOptions _options;
        private readonly SemaphoreSlim _handlerSemaphore;

        private CancellationTokenSource? _cancellationTokenSource;
        private Task? _listenerTask;
        private bool _disposed;

        public bool IsRunning => _listenerTask != null && !_listenerTask.IsCompleted;

        public KafkaEventListener(
            KafkaListenerOptions options,
            IEventStreamSerializer<T>? serializer = null,
            ILogger<KafkaEventListener<T>>? logger = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();

            _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<KafkaEventListener<T>>.Instance;
            _handlerSemaphore = new SemaphoreSlim(_options.ConcurrentHandlers, _options.ConcurrentHandlers);

            // Create consumer with the same options
            var consumerOptions = new KafkaConsumerOptions
            {
                ConnectionString = _options.ConnectionString,
                GroupId = _options.GroupId,
                Properties = _options.Properties,
                EnableAutoCommit = _options.EnableAutoCommit,
                AutoCommitInterval = _options.AutoCommitInterval,
                MaxPollRecords = _options.MaxPollRecords,
                SessionTimeout = _options.SessionTimeout,
                AutoOffsetReset = _options.AutoOffsetReset,
                EnablePartitionEof = _options.EnablePartitionEof,
                FetchMinBytes = _options.FetchMinBytes,
                FetchMaxWaitMs = _options.FetchMaxWaitMs
            };

            _consumer = new KafkaEventStreamConsumer<T>(consumerOptions, serializer,
                logger as ILogger<KafkaEventStreamConsumer<T>>);
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (IsRunning)
            {
                _logger.LogWarning("Event listener is already running");
                return;
            }

            if (!_handlers.Any())
            {
                throw new InvalidOperationException("No event handlers registered. Use Subscribe() to register handlers before starting.");
            }

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            // Subscribe to all topics that have handlers
            var topics = _handlers.Keys.ToList();
            await _consumer.SubscribeAsync(topics, cancellationToken);

            _listenerTask = Task.Run(() => ListenLoop(_cancellationTokenSource.Token), cancellationToken);

            _logger.LogInformation("Event listener started for topics: {Topics}", string.Join(", ", topics));
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (!IsRunning)
            {
                _logger.LogWarning("Event listener is not running");
                return;
            }

            _logger.LogInformation("Stopping event listener...");

            _cancellationTokenSource?.Cancel();

            if (_listenerTask != null)
            {
                try
                {
                    await _listenerTask.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancellation is requested
                }
            }

            _consumer.Unsubscribe();

            _logger.LogInformation("Event listener stopped");
        }

        public IEventListener<T> Subscribe(string topic, EventHandlerAsync<T> handler)
        {
            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException("Topic cannot be null or empty", nameof(topic));

            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            if (IsRunning)
                throw new InvalidOperationException("Cannot subscribe to topics while listener is running. Stop the listener first.");

            _handlers.AddOrUpdate(topic, handler, (key, existing) => handler);

            _logger.LogDebug("Subscribed to topic: {Topic}", topic);

            return this;
        }

        public IEventListener<T> OnError(ErrorHandlerAsync errorHandler)
        {
            if (errorHandler == null)
                throw new ArgumentNullException(nameof(errorHandler));

            _errorHandlers.Add(errorHandler);

            _logger.LogDebug("Error handler registered");

            return this;
        }

        private async Task ListenLoop(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Starting listen loop");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = await _consumer.ConsumeAsync(cancellationToken);

                    if (!result.IsSuccess)
                    {
                        await HandleError(result.Exception ?? new Exception(result.ErrorMessage), null, cancellationToken);

                        if (!_options.ContinueOnError)
                            break;

                        await Task.Delay(_options.ErrorDelay, cancellationToken);
                        continue;
                    }

                    var message = result.Data;
                    if (message == null)
                        continue;

                    // Process message with concurrency control
                    _ = Task.Run(async () =>
                    {
                        await _handlerSemaphore.WaitAsync(cancellationToken);
                        try
                        {
                            await ProcessMessage(message, cancellationToken);
                        }
                        finally
                        {
                            _handlerSemaphore.Release();
                        }
                    }, cancellationToken);

                    // Auto-commit if enabled
                    if (!_options.EnableAutoCommit)
                    {
                        await _consumer.CommitAsync(message, cancellationToken);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Listen loop cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    await HandleError(ex, null, cancellationToken);

                    if (!_options.ContinueOnError)
                        break;

                    await Task.Delay(_options.ErrorDelay, cancellationToken);
                }
            }

            _logger.LogDebug("Listen loop ended");
        }

        private async Task ProcessMessage(EventStreamMessage<T> message, CancellationToken cancellationToken)
        {
            if (!_handlers.TryGetValue(message.Topic, out var handler))
            {
                _logger.LogWarning("No handler found for topic: {Topic}", message.Topic);
                return;
            }

            var retryCount = 0;

            while (retryCount <= _options.MaxRetries)
            {
                try
                {
                    _logger.LogDebug("Processing message {MessageId} from topic {Topic} (attempt {Attempt})",
                        message.Id, message.Topic, retryCount + 1);

                    await handler(message, cancellationToken);

                    _logger.LogDebug("Successfully processed message {MessageId} from topic {Topic}",
                        message.Id, message.Topic);

                    return;
                }
                catch (Exception ex)
                {
                    retryCount++;

                    _logger.LogError(ex, "Failed to process message {MessageId} from topic {Topic} (attempt {Attempt}/{MaxAttempts})",
                        message.Id, message.Topic, retryCount, _options.MaxRetries + 1);

                    if (retryCount <= _options.MaxRetries)
                    {
                        await Task.Delay(_options.ErrorDelay, cancellationToken);
                    }
                    else
                    {
                        await HandleError(ex, message.Topic, cancellationToken);
                        return;
                    }
                }
            }
        }

        private async Task HandleError(Exception exception, string? topic, CancellationToken cancellationToken)
        {
            _logger.LogError(exception, "Error occurred in event listener for topic: {Topic}", topic ?? "unknown");

            foreach (var errorHandler in _errorHandlers)
            {
                try
                {
                    await errorHandler(exception, topic, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in error handler");
                }
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(KafkaEventListener<T>));
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                StopAsync().GetAwaiter().GetResult();

                _cancellationTokenSource?.Dispose();
                _consumer?.Dispose();
                _handlerSemaphore?.Dispose();

                _disposed = true;
            }
        }
    }
}
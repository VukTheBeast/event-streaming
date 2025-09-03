using EventStreaming.Core.Models;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EventStreaming.Core.Abstractions
{
    public interface IEventStreamConsumer<T> : IDisposable
    {
        Task SubscribeAsync(
            string topic, 
            CancellationToken cancellationToken = default);

        Task SubscribeAsync(
            IEnumerable<string> topics, 
            CancellationToken cancellationToken = default);

        Task<EventStreamResult<EventStreamMessage<T>?>> ConsumeAsync(
            CancellationToken cancellationToken = default);

        Task<IEnumerable<EventStreamResult<EventStreamMessage<T>>>> ConsumeBatchAsync(
            int maxMessages = 100, 
            TimeSpan? timeout = null, 
            CancellationToken cancellationToken = default);

        Task CommitAsync(CancellationToken cancellationToken = default);

        Task CommitAsync(
            EventStreamMessage<T> message, 
            CancellationToken cancellationToken = default);

        void Unsubscribe();
    }
}

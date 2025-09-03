using EventStreaming.Core.Models;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EventStreaming.Core.Abstractions
{
    public interface IEventStreamProducer<T> : IDisposable
    {
        Task<EventStreamResult> PublishAsync(
            string topic, 
            T data, 
            string? key = null, 
            CancellationToken cancellationToken = default);

        Task<EventStreamResult> PublishAsync(
            EventStreamMessage<T> message, 
            CancellationToken cancellationToken = default);

        Task<IEnumerable<EventStreamResult>> PublishBatchAsync(
            string topic, 
            IEnumerable<T> data, 
            CancellationToken cancellationToken = default);

        Task<IEnumerable<EventStreamResult>> PublishBatchAsync(
            IEnumerable<EventStreamMessage<T>> messages, 
            CancellationToken cancellationToken = default);

        Task FlushAsync(CancellationToken cancellationToken = default);
    }
}
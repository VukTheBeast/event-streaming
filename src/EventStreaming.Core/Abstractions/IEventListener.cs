using EventStreaming.Core.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStreaming.Core.Abstractions
{
    public delegate Task EventHandlerAsync<T>(EventStreamMessage<T> message, CancellationToken cancellationToken);
    public delegate Task ErrorHandlerAsync(Exception exception, string? topic, CancellationToken cancellationToken);

    public interface IEventListener<T> : IDisposable
    {
        Task StartAsync(CancellationToken cancellationToken = default);
        Task StopAsync(CancellationToken cancellationToken = default);
        
        IEventListener<T> Subscribe(string topic, EventHandlerAsync<T> handler);
        IEventListener<T> OnError(ErrorHandlerAsync errorHandler);
        
        bool IsRunning { get; }
    }
}
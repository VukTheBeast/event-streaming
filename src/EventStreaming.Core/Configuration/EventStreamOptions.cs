using EventStreaming.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace EventStreaming.Core.Configuration
{
    public abstract class EventStreamOptions : IEventStreamConfiguration
    {
        [Required]
        public string ConnectionString { get; set; } = string.Empty;

        public string? GroupId { get; set; }

        public Dictionary<string, string> Properties { get; set; } = new();

        public virtual void Validate()
        {
            if (string.IsNullOrWhiteSpace(ConnectionString))
                throw new ArgumentException("ConnectionString is required", nameof(ConnectionString));
        }
    }

    public class ProducerOptions : EventStreamOptions
    {
        public bool EnableIdempotence { get; set; } = true;
        public int BatchSize { get; set; } = 100;
        public TimeSpan FlushTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public int RetryCount { get; set; } = 3;
    }

    public class ConsumerOptions : EventStreamOptions
    {
        public bool EnableAutoCommit { get; set; } = false;
        public TimeSpan AutoCommitInterval { get; set; } = TimeSpan.FromSeconds(5);
        public int MaxPollRecords { get; set; } = 500;
        public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromSeconds(30);

        public override void Validate()
        {
            base.Validate();
            
            if (string.IsNullOrWhiteSpace(GroupId))
                throw new ArgumentException("GroupId is required for consumers", nameof(GroupId));
        }
    }

    public class ListenerOptions : ConsumerOptions
    {
        public int ConcurrentHandlers { get; set; } = 1;
        public bool ContinueOnError { get; set; } = true;
        public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);
        public int MaxRetries { get; set; } = 3;
    }
}
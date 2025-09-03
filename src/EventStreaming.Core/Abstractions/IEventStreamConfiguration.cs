using System.Collections.Generic;

namespace EventStreaming.Core.Abstractions
{
    public interface IEventStreamConfiguration
    {
        string ConnectionString { get; }
        string? GroupId { get; }
        Dictionary<string, string> Properties { get; }
        
        void Validate();
    }
}
using System;
using System.Threading.Tasks;

namespace EventStreaming.Kafka.Serialization
{
    public interface IEventStreamSerializer<T>
    {
        byte[] Serialize(T data);
        T? Deserialize(byte[] data);
    }

    public interface IAsyncEventStreamSerializer<T>
    {
        Task<byte[]> SerializeAsync(T data);
        Task<T?> DeserializeAsync(byte[] data);
    }
}
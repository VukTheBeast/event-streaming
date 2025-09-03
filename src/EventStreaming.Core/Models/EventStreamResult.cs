using System;

namespace EventStreaming.Core.Models
{
    public class EventStreamResult
    {
        public bool IsSuccess { get; set; }
        public string? ErrorMessage { get; set; }
        public Exception? Exception { get; set; }
        public string? MessageId { get; set; }
        public long? Offset { get; set; }

        public EventStreamResult() { }

        public static EventStreamResult Success(string? messageId = null, long? offset = null)
        {
            return new EventStreamResult
            {
                IsSuccess = true,
                MessageId = messageId,
                Offset = offset
            };
        }

        public static EventStreamResult Failure(string errorMessage, Exception? exception = null)
        {
            return new EventStreamResult
            {
                IsSuccess = false,
                ErrorMessage = errorMessage,
                Exception = exception
            };
        }

        public static EventStreamResult Failure(Exception exception)
        {
            return new EventStreamResult
            {
                IsSuccess = false,
                ErrorMessage = exception.Message,
                Exception = exception
            };
        }
    }

    public class EventStreamResult<T> : EventStreamResult
    {
        public T? Data { get; private set; }

        private EventStreamResult() { }

        public static EventStreamResult<T> Success(T data, string? messageId = null, long? offset = null)
        {
            return new EventStreamResult<T>
            {
                IsSuccess = true,
                Data = data,
                MessageId = messageId,
                Offset = offset
            };
        }

        public new static EventStreamResult<T> Failure(string errorMessage, Exception? exception = null)
        {
            return new EventStreamResult<T>
            {
                IsSuccess = false,
                ErrorMessage = errorMessage,
                Exception = exception
            };
        }

        public new static EventStreamResult<T> Failure(Exception exception)
        {
            return new EventStreamResult<T>
            {
                IsSuccess = false,
                ErrorMessage = exception.Message,
                Exception = exception
            };
        }
    }
}
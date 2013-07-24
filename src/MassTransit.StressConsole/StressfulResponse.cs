namespace MassTransit.StressConsole
{
    using System;


    public interface StressfulResponse
    {
        Guid ResponseId { get; }
        DateTime Timestamp { get; }
        Guid RequestId { get; }
    }
}
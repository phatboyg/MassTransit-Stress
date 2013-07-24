namespace MassTransit.StressConsole
{
    using System;


    public interface StressfulRequest
    {
        Guid RequestId { get; }
        DateTime Timestamp { get; }
    }
}
namespace MassTransit.StressConsole
{
    using System;
    using System.Threading;
    using Topshelf;
    using Topshelf.Logging;


    class Program
    {
        static int Main()
        {
            Log4NetLogWriterFactory.Use("log4net.config");

            string username = "guest";
            string password = "guest";
            var serviceBusUri = new Uri("rabbitmq://localhost/stress_service?prefetch=32");
            ushort heartbeat = 3;
            int iterations = 1000;
            int instances = 10;

            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            ThreadPool.SetMinThreads(Math.Max(workerThreads, 100), completionPortThreads);

            return (int)HostFactory.Run(x =>
                {
                    x.SetDescription("Generates a stressful load on RabbitMQ using MassTransit");
                    x.SetDisplayName("MassTransit RabbitMQ Stress Console");
                    x.SetServiceName("MassTransitStressConsole");

                    x.AddCommandLineDefinition("username", v => username = v);
                    x.AddCommandLineDefinition("password", v => password = v);
                    x.AddCommandLineDefinition("uri", v => serviceBusUri = new Uri(v));
                    x.AddCommandLineDefinition("heartbeat", v => heartbeat = ushort.Parse(v));
                    x.AddCommandLineDefinition("iterations", v => iterations = int.Parse(v));
                    x.AddCommandLineDefinition("instances", v => instances = int.Parse(v));

                    x.Service(hostSettings => new StressService(serviceBusUri, username, password, heartbeat, iterations, instances));
                });
        }
    }
}
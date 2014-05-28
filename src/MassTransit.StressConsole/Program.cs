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
            var serviceBusUri = new Uri("rabbitmq://localhost/stress_service");
            ushort heartbeat = 3;
            int iterations = 1000;
            int instances = 10;
            int messageSize = 128;
            bool cleanup = true;
            bool mixed = false;

            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            ThreadPool.SetMinThreads(Math.Max(workerThreads, 100), completionPortThreads);

            return (int)HostFactory.Run(x =>
                {
                    x.SetDescription("Generates a stressful load on RabbitMQ using MassTransit");
                    x.SetDisplayName("MassTransit RabbitMQ Stress Console");
                    x.SetServiceName("MassTransitStressConsole");

                    x.AddCommandLineDefinition("rmqusername", v => username = v);
                    x.AddCommandLineDefinition("rmqpassword", v => password = v);
                    x.AddCommandLineDefinition("uri", v => serviceBusUri = new Uri(v));
                    x.AddCommandLineDefinition("heartbeat", v => heartbeat = ushort.Parse(v));
                    x.AddCommandLineDefinition("iterations", v => iterations = int.Parse(v));
                    x.AddCommandLineDefinition("instances", v => instances = int.Parse(v));
                    x.AddCommandLineDefinition("size", v => messageSize = int.Parse(v));
                    x.AddCommandLineDefinition("cleanup", v => cleanup = bool.Parse(v));
                    x.AddCommandLineDefinition("mixed", v => mixed = bool.Parse(v));
                    x.ApplyCommandLine();

                    x.Service(hostSettings => new StressService(serviceBusUri, username, password, heartbeat,
                                                  iterations, instances, messageSize, cleanup, mixed));
                });
        }
    }
}
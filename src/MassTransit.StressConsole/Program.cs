namespace MassTransit.StressConsole
{
    using System;
    using System.Linq;
    using System.Threading;
    using log4net;
    using log4net.Core;
    using log4net.Repository;
    using log4net.Repository.Hierarchy;
    using Log4NetIntegration.Logging;
    using Magnum.Extensions;
    using Topshelf;
    using Topshelf.Logging;


    class Program
    {
        static int Main()
        {
            Log4NetLogWriterFactory.Use("log4net.config");
            Log4NetLogger.Use();

            string username = "guest";
            string password = "guest";
            var serviceBusUri = new Uri("rabbitmq://localhost/stress_service");
            ushort heartbeat = 3;
            int iterations = 1000;
            int instances = 10;
            int messageSize = 128;
            int prefetchCount = 10;
            int consumerLimit = 10;
            string test = "stress";
            bool cleanup = true;
            bool mixed = false;
            string debug = null;


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
                x.AddCommandLineDefinition("prefetch", v => prefetchCount = int.Parse(v));
                x.AddCommandLineDefinition("threads", v => consumerLimit = int.Parse(v));
                x.AddCommandLineDefinition("test", v => test = v);
                x.AddCommandLineDefinition("size", v => messageSize = int.Parse(v));
                x.AddCommandLineDefinition("cleanup", v => cleanup = bool.Parse(v));
                x.AddCommandLineDefinition("mixed", v => mixed = bool.Parse(v));
                x.AddCommandLineDefinition("debug", v => debug = v);
                x.ApplyCommandLine();

                x.Service(hostSettings =>
                {
                    if (!string.IsNullOrWhiteSpace(debug))
                        EnableDebug(debug);

                    if (test == "ingest")
                    {
                        return new SelectService(new StressIngestService(serviceBusUri, username, password, heartbeat,
                            iterations, instances, messageSize, cleanup, mixed, prefetchCount, consumerLimit));
                    }


                    return new SelectService(new StressService(serviceBusUri, username, password, heartbeat,
                        iterations, instances, messageSize, cleanup, mixed, prefetchCount, consumerLimit));
                });
            });
        }

        static void EnableDebug(string debug)
        {
            Console.WriteLine("Enabling debug for {0}", debug);

            ILoggerRepository[] repositories = LogManager.GetAllRepositories();
            foreach (Hierarchy hierarchy in repositories)
            {
                ILogger[] loggers = hierarchy.GetCurrentLoggers();
                foreach (Logger logger in loggers)
                {
                    if (string.Compare(logger.Name, debug, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        Console.WriteLine("Setting {0} to DEBUG", logger.Name);
                        logger.Level = hierarchy.LevelMap["DEBUG"];
                        return;
                    }
                }
            }

            Console.WriteLine("Logger not found, adding and updating");

            var log = (Logger)LogManager.GetLogger(debug);
            log.Level = repositories.First().CastAs<Hierarchy>().LevelMap["DEBUG"];
        }
    }
}
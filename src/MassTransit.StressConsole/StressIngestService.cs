namespace MassTransit.StressConsole
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Magnum.Extensions;
    using RabbitMQ.Client;
    using RabbitMQ.Client.Exceptions;
    using Taskell;
    using Topshelf;
    using Topshelf.Logging;
    using Transports.RabbitMq;


    class StressIngestService :
        ServiceControl
    {
        readonly CancellationTokenSource _cancel;
        readonly bool _cleanUp;
        readonly Uri _clientUri;
        readonly int _consumerLimit;
        readonly ushort _heartbeat;
        readonly int _instances;
        readonly int _iterations;
        readonly LogWriter _log = HostLogger.Get<StressIngestService>();
        readonly string _messageContent;
        readonly int _messageSize;
        readonly bool _mixed;
        readonly string _password;
        readonly Uri _serviceBusUri;
        readonly string _username;
        IList<Task> _clientTasks;
        Stopwatch _generatorStartTime;
        HostControl _hostControl;
        int _instanceCount;
        int _sendCount;
        long _sendTime;
        RabbitMqEndpointAddress _serviceBusAddress;
        int[][] _timings;
        long _totalTime;

        public StressIngestService(Uri serviceBusUri, string username, string password, ushort heartbeat, int iterations,
            int instances, int messageSize, bool cleanUp, bool mixed, int prefetchCount, int consumerLimit)
        {
            _username = username;
            _password = password;
            _heartbeat = heartbeat;
            _iterations = iterations;
            _instances = instances;
            _messageSize = messageSize;
            _consumerLimit = consumerLimit;
            _cleanUp = cleanUp;
            _mixed = mixed;
            _serviceBusUri = serviceBusUri;
            _messageContent = new string('*', messageSize);

            _clientUri = _serviceBusUri;

            _cancel = new CancellationTokenSource();
            _clientTasks = new List<Task>();
        }

        public bool Start(HostControl hostControl)
        {
            _hostControl = hostControl;

            _log.InfoFormat("RabbitMQ Ingest Stress Test (using MassTransit)");

            _log.InfoFormat("Host: {0}", _serviceBusUri);
            _log.InfoFormat("Username: {0}", _username);
            _log.InfoFormat("Password: {0}", new String('*', _password.Length));
            _log.InfoFormat("Message Size: {0} {1}", _messageSize, _mixed ? "(mixed)" : "(fixed)");
            _log.InfoFormat("Iterations: {0}", _iterations);
            _log.InfoFormat("Clients: {0}", _instances);
            _log.InfoFormat("Heartbeat: {0}", _heartbeat);


            _log.InfoFormat("Creating {0}", _serviceBusUri);

            IServiceBus serviceBus = ServiceBusFactory.New(x =>
            {
                x.UseRabbitMq(r =>
                {
                    r.ConfigureHost(_serviceBusUri, h =>
                    {
                        h.SetUsername(_username);
                        h.SetPassword(_password);
                        h.SetRequestedHeartbeat(_heartbeat);
                    });
                });

                x.ReceiveFrom(_serviceBusUri);
                x.SetConcurrentConsumerLimit(_consumerLimit);

                x.Subscribe(s => s.Handler<StressfulRequest>((context, message) =>
                {
                    // just respond with the Id
                    context.Respond(new StressfulResponseMessage(message.RequestId));
                }));
            });
            _serviceBusAddress = serviceBus.Endpoint.Address as RabbitMqEndpointAddress;
            serviceBus.Dispose();

            // this just ensures the queues are setup right

            _generatorStartTime = Stopwatch.StartNew();
            StartStressGenerators().Wait(_cancel.Token);

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            bool wait = Task.WaitAll(_clientTasks.ToArray(), (_iterations * _instances / 100).Seconds());
            if (wait)
            {
                _generatorStartTime.Stop();

                _log.InfoFormat("RabbitMQ Stress Test Completed");
                _log.InfoFormat("Send Count: {0}", _sendCount);
                _log.InfoFormat("Average Send Time: {0}ms", TimeSpan.FromTicks(_sendTime).TotalMilliseconds / _sendCount);

                _log.InfoFormat("Max Response Time: {0}ms", TimeSpan.FromTicks(_timings.SelectMany(x => x).Max()).TotalMilliseconds);
                _log.InfoFormat("Min Response Time: {0}ms", TimeSpan.FromTicks(_timings.SelectMany(x => x).Min()).TotalMilliseconds);
                double? median = _timings.SelectMany(x => x).Median();
                if(median.HasValue)
                _log.InfoFormat("Med Response Time: {0}ms", TimeSpan.FromTicks((long)median.Value).TotalMilliseconds);
                double? percentile = _timings.SelectMany(x => x).Percentile(95);
                if(percentile.HasValue)
                    _log.InfoFormat("95% Response Time: {0}ms", TimeSpan.FromTicks((long)percentile.Value).TotalMilliseconds);

                _log.InfoFormat("Elapsed Test Time: {0}", _generatorStartTime.Elapsed);
                _log.InfoFormat("Total Client Time: {0}ms", _totalTime);
                _log.InfoFormat("Per Client Time: {0}ms", _totalTime / _instances);
                _log.InfoFormat("Message Throughput: {0}m/s",
                    (_sendCount) * 1000 / (_totalTime / _instances));

                DrawResponseTimeGraph();
            }

            _cancel.Cancel();

            if (_cleanUp)
                CleanUpQueuesAndExchanges();

            return wait;
        }

        void CleanUpQueuesAndExchanges()
        {
            RabbitMqEndpointAddress address = RabbitMqEndpointAddress.Parse(_serviceBusUri);
            ConnectionFactory connectionFactory = address.ConnectionFactory;
            if (string.IsNullOrWhiteSpace(connectionFactory.UserName))
                connectionFactory.UserName = "guest";
            if (string.IsNullOrWhiteSpace(connectionFactory.Password))
                connectionFactory.Password = "guest";

            using (IConnection connection = connectionFactory.CreateConnection())
            {
                using (IModel model = connection.CreateModel())
                {
                    model.ExchangeDelete(address.Name);
                    model.QueueDelete(address.Name);

                    for (int i = 0; i < 10000; i++)
                    {
                        string name = string.Format("{0}_client_{1}", address.Name, i);
                        try
                        {
                            model.QueueDeclarePassive(name);
                        }
                        catch (OperationInterruptedException)
                        {
                            break;
                        }

                        model.ExchangeDelete(name);
                        model.QueueDelete(name);
                    }
                }
            }
        }

        void DrawResponseTimeGraph()
        {
            int maxTime = _timings.SelectMany(x => x).Select(x => x / 100).Max();
            int minTime = _timings.SelectMany(x => x).Select(x => x / 100).Min();

            const int segments = 10;

            int span = maxTime - minTime;
            int increment = span / segments;

            var histogram = (from x in _timings.SelectMany(x => x).Select(x => x / 100)
                let key = ((x - minTime) * segments / span)
                where key >= 0 && key < segments
                let groupKey = key
                group x by groupKey
                into segment
                orderby segment.Key
                select new {Value = segment.Key, Count = segment.Count()}).ToList();

            int maxCount = histogram.Max(x => x.Count);

            foreach (var item in histogram)
            {
                int barLength = item.Count * 60 / maxCount;
                _log.InfoFormat("{0,5}ms {2,-60} ({1,7})", TimeSpan.FromTicks(minTime * 100 + increment * item.Value * 100).TotalMilliseconds, item.Count,
                    new string('*', barLength));
            }
        }

        async Task StartStressGenerators()
        {
            var start = new TaskCompletionSource<bool>();

            var starting = new List<Task>();
            _timings = new int[_instances][];
            for (int i = 0; i < _instances; i++)
            {
                _timings[i] = new int[_iterations];
                starting.Add(StartStressGenerator(i, start.Task));
            }

            await Task.WhenAll(starting.ToArray());

            start.TrySetResult(true);
        }

        Task StartStressGenerator(int instance, Task start)
        {
            var ready = new TaskCompletionSource<bool>();

            var composer = new TaskComposer<bool>(_cancel.Token, false);

            string queueName = string.Format("{0}_client_{1}", _serviceBusAddress.Name, instance);
            Uri uri = RabbitMqEndpointAddress.Parse(_clientUri).ForQueue(queueName).Uri;

            var uriBuilder = new UriBuilder(uri);
            uriBuilder.Query = _clientUri.Query.Trim('?');

            Uri address = uriBuilder.Uri;

            composer.Execute(() => { Interlocked.Increment(ref _instanceCount); });

            composer.Execute(async () => { await Task.Yield(); });

            IServiceBus bus = null;
            composer.Execute(() =>
            {
                _log.InfoFormat("Creating {0}", address);

                bus = ServiceBusFactory.New(x =>
                {
                    x.UseRabbitMq(r =>
                    {
                        r.ConfigureHost(address, h =>
                        {
                            h.SetUsername(_username);
                            h.SetPassword(_password);
                            h.SetRequestedHeartbeat(_heartbeat);
                        });
                    });

                    x.ReceiveFrom(address);
                });
            }, false);

            Stopwatch clientTimer = null;

            composer.Execute(async () =>
            {
                ready.TrySetResult(true);
                await start;
            });

            composer.Execute(() => clientTimer = Stopwatch.StartNew());

            composer.Execute(() =>
            {
                Task task = composer.Compose(x =>
                {
                    for (int i = 0; i < _iterations; i++)
                    {
                        int iteration = i;
                        x.Execute(() =>
                        {
                            string messageContent = _mixed && iteration % 2 == 0
                                ? new string('*', 128)
                                : _messageContent;
                            var requestMessage = new StressfulRequestMessage(messageContent);

                            Stopwatch sendTimer = Stopwatch.StartNew();

                            bus.Publish<StressfulRequest>(requestMessage);

                            sendTimer.Stop();

                            Interlocked.Add(ref _sendTime, sendTimer.ElapsedTicks);
                            _timings[instance][iteration] = (int)sendTimer.ElapsedTicks;

                            Interlocked.Increment(ref _sendCount);
                        });
                    }
                });

                return task;
            });

            composer.Execute(() => clientTimer.Stop());

            composer.Execute(() => bus.Dispose(), false);

            composer.Compensate(compensation => { return compensation.Handled(); });

            composer.Finally(status =>
            {
                Interlocked.Add(ref _totalTime, clientTimer.ElapsedMilliseconds);
                int count = Interlocked.Decrement(ref _instanceCount);
                if (count == 0)
                    Task.Factory.StartNew(() => _hostControl.Stop());
            }, false);

            _clientTasks.Add(composer.Finish());

            return ready.Task;
        }


        class StressfulRequestMessage :
            StressfulRequest
        {
            public StressfulRequestMessage(string content)
            {
                RequestId = NewId.NextGuid();
                Timestamp = DateTime.UtcNow;
                Content = content;
            }

            public Guid RequestId { get; private set; }
            public DateTime Timestamp { get; private set; }
            public string Content { get; private set; }
        }


        class StressfulResponseMessage :
            StressfulResponse
        {
            public StressfulResponseMessage(Guid requestId)
            {
                ResponseId = NewId.NextGuid();
                Timestamp = DateTime.UtcNow;

                RequestId = requestId;
            }

            public Guid ResponseId { get; private set; }
            public DateTime Timestamp { get; private set; }
            public Guid RequestId { get; private set; }
        }
    }
}
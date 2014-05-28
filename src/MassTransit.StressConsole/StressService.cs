namespace MassTransit.StressConsole
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Magnum.Extensions;
    using RabbitMQ.Client;
    using RabbitMQ.Client.Exceptions;
    using Taskell;
    using Topshelf;
    using Topshelf.Logging;
    using Transports.RabbitMq;


    class StressService :
        ServiceControl
    {
        readonly CancellationTokenSource _cancel;
        readonly ushort _heartbeat;
        readonly int _instances;
        readonly int _iterations;
        readonly LogWriter _log = HostLogger.Get<StressService>();
        readonly int _messageSize;
        readonly bool _cleanUp;
        readonly bool _mixed;
        readonly string _password;
        readonly Uri _serviceBusUri;
        readonly string _username;
        IList<Task> _clientTasks;
        Stopwatch _generatorStartTime;
        HostControl _hostControl;
        int _instanceCount;
        int _requestCount;
        int _responseCount;
        long _responseTime;
        IServiceBus _serviceBus;
        int[][] _timings;
        long _totalTime;
        readonly string _messageContent;

        public StressService(Uri serviceBusUri, string username, string password, ushort heartbeat, int iterations, int instances, int messageSize, bool cleanUp, bool mixed)
        {
            _username = username;
            _password = password;
            _heartbeat = heartbeat;
            _iterations = iterations;
            _instances = instances;
            _messageSize = messageSize;
            _cleanUp = cleanUp;
            _mixed = mixed;
            _serviceBusUri = serviceBusUri;
            _messageContent = new string('*', messageSize);

            var prefetch = new Regex(@"([\?\&])prefetch=[^\&]+[\&]?");
            string query = _serviceBusUri.Query;

            if (query.IndexOf("prefetch", StringComparison.InvariantCultureIgnoreCase) >= 0)
                query = prefetch.Replace(query, string.Format("prefetch={0}", _instances));
            else if (string.IsNullOrEmpty(query))
                query = string.Format("prefetch={0}", _instances);
            else
                query += string.Format("&prefetch={0}", _instances);

            var builder = new UriBuilder(_serviceBusUri);
            builder.Query = query.Trim('?');
            _serviceBusUri = builder.Uri;

            _cancel = new CancellationTokenSource();
            _clientTasks = new List<Task>();
        }

        public bool Start(HostControl hostControl)
        {
            _hostControl = hostControl;

            _log.InfoFormat("Running {0} iterations with {1} client instances", _iterations, _instances);
            _log.InfoFormat("Using a content size of {0} bytes", _messageSize);

            _log.InfoFormat("Creating service bus at {0}", _serviceBusUri);

            _serviceBus = ServiceBusFactory.New(x =>
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
                    x.SetConcurrentConsumerLimit(_instances);

                    x.Subscribe(s => s.Handler<StressfulRequest>((context, message) =>
                        {
                            // just respond with the Id
                            context.Respond(new StressfulResponseMessage(message.RequestId));
                        }));
                });

            _generatorStartTime = Stopwatch.StartNew();
            StartStressGenerators();

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            bool wait = Task.WaitAll(_clientTasks.ToArray(), (_iterations * _instances / 100).Seconds());
            if (wait)
            {
                _generatorStartTime.Stop();

                _log.InfoFormat("Stress completed");
                _log.InfoFormat("Request Count: {0}", _requestCount);
                _log.InfoFormat("Response Count: {0}", _responseCount);
                _log.InfoFormat("Average Response Time: {0}ms", _responseTime / _responseCount);

                _log.InfoFormat("Max Response Time: {0}ms", _timings.SelectMany(x => x).Max());
                _log.InfoFormat("Min Response Time: {0}ms", _timings.SelectMany(x => x).Min());
                _log.InfoFormat("Med Response Time: {0}ms", (int?)_timings.SelectMany(x => x).Median());
                _log.InfoFormat("95% Response Time: {0}ms", (int?)_timings.SelectMany(x => x).Percentile(95));

                _log.InfoFormat("Elapsed Test Time: {0}", _generatorStartTime.Elapsed);
                _log.InfoFormat("Total Client Time: {0}ms", _totalTime);
                _log.InfoFormat("Per Client Time: {0}ms", _totalTime / _instances);
                _log.InfoFormat("Message Throughput: {0}m/s",
                    (_requestCount + _responseCount) * 1000 / (_totalTime / _instances));

                DrawResponseTimeGraph();
            }

            _cancel.Cancel();

            if (_serviceBus != null)
            {
                _serviceBus.Dispose();
                _serviceBus = null;
            }

            if (_cleanUp)
            {
                CleanUpQueuesAndExchanges();
            }

            return wait;
        }

        void CleanUpQueuesAndExchanges()
        {
            var address = RabbitMqEndpointAddress.Parse(_serviceBusUri);
            var connectionFactory = address.ConnectionFactory;
            if (string.IsNullOrWhiteSpace(connectionFactory.UserName))
                connectionFactory.UserName = "guest";
            if (string.IsNullOrWhiteSpace(connectionFactory.Password))
                connectionFactory.Password = "guest";

            using (var connection = connectionFactory.CreateConnection())
            {
                using (var model = connection.CreateModel())
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
            var maxTime = _timings.SelectMany(x => x).Max();
            var minTime = _timings.SelectMany(x => x).Min();

            const int segments = 10;

            var span = maxTime - minTime;
            var increment = span / segments;

            var histogram = (from x in _timings.SelectMany(x => x)
                        let key = ((x - minTime) * segments / span)
                        where key >= 0 && key < segments
                        let groupKey = key
                        group x by groupKey into segment
                        orderby segment.Key
                        select new { Value = segment.Key, Count = segment.Count() }).ToList();

            var maxCount = histogram.Max(x => x.Count);

            foreach (var item in histogram)
            {
                int barLength = item.Count * 60 / maxCount;
                _log.InfoFormat("{0,5}ms {2,-60} ({1,7})", minTime + increment * item.Value, item.Count, new string('*', barLength));
            }
        }

        void StartStressGenerators()
        {
            _timings = new int[_instances][];
            for (int i = 0; i < _instances; i++)
            {
                _timings[i] = new int[_iterations];
                StartStressGenerator(i);
            }
        }

        void StartStressGenerator(int instance)
        {
            var composer = new TaskComposer<bool>(_cancel.Token, false);

            var endpointAddress = _serviceBus.Endpoint.Address as IRabbitMqEndpointAddress;
            string queueName = string.Format("{0}_client_{1}", endpointAddress.Name, instance);
            Uri uri = RabbitMqEndpointAddress.Parse(_serviceBusUri).ForQueue(queueName).Uri;

            var uriBuilder = new UriBuilder(uri);
            uriBuilder.Query = _serviceBusUri.Query.Trim('?');

            Uri address = uriBuilder.Uri;

            composer.Execute(() => { Interlocked.Increment(ref _instanceCount); });

            IServiceBus bus = null;
            composer.Execute(() =>
                {
                    _log.InfoFormat("Creating stress client at {0}", address);

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
                                        var messageContent = _mixed && iteration % 2 == 0 ? new string('*', 128) : _messageContent;
                                        var requestMessage = new StressfulRequestMessage(messageContent);
                                        ITaskRequest<StressfulRequest> taskRequest =
                                            bus.PublishRequestAsync<StressfulRequest>(requestMessage, r =>
                                                {
                                                    r.Handle<StressfulResponse>(response =>
                                                        {
                                                            Interlocked.Increment(ref _responseCount);

                                                            TimeSpan timeSpan = response.Timestamp
                                                                                - requestMessage.Timestamp;
                                                            Interlocked.Add(ref _responseTime,
                                                                (long)timeSpan.TotalMilliseconds);
                                                            _timings[instance][iteration] =
                                                                (int)timeSpan.TotalMilliseconds;
                                                        });
                                                });

                                        Interlocked.Increment(ref _requestCount);

                                        return taskRequest.Task;
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
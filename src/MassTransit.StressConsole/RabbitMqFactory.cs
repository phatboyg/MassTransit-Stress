namespace MassTransit.StressConsole
{
    using System;
    using Transports.RabbitMq.Configuration.Configurators;


    public class RabbitMqEndpointCacheFactory : IDisposable
    {
        private readonly IRabbitMqConfig _config;
        private IEndpointCache _endpointCache;
        private bool _disposed;

        /// <summary>
        /// Inititalize rabbitmq endpoint with configuration
        /// </summary>
        /// <param name="config">Rabbit mq Configuration</param>
        public RabbitMqEndpointCacheFactory(IRabbitMqConfig config)
        {
            _config = config;

            validateConfig(config);
        }

        /// <summary>
        /// Configure rabbit mq configuration for username, password, heartbeat, and uri
        /// </summary>
        /// <param name="configurator">Rabbit mq configuration</param>
        /// <param name="busUri">Queue uri</param>
        void configure(RabbitMqTransportFactoryConfigurator configurator, Uri busUri)
        {
            configurator.ConfigureHost(busUri, h =>
            {
                h.SetRequestedHeartbeat(_config.HeartBeat);
                h.SetUsername(_config.User);
                h.SetPassword(_config.Password);
            });
        }


        /// <summary>
        /// Returns the endpoint from which the messages are received
        /// </summary>
        public IEndpoint GetEndpoint(Uri address)
        {
                if (_endpointCache != null)
                    return _endpointCache.GetEndpoint(address);

                Initialize();

                return _endpointCache.GetEndpoint(address);
        }

        /// <summary>
        /// Initializes the end point with the configured rabbit mq information;
        /// </summary>
        public void Initialize()
        {
            if (_endpointCache == null)
            {
                var busUri = uri();

                _endpointCache = EndpointCacheFactory.New(sbc =>
                {
                    sbc.UseRabbitMq(config => configure(config, busUri));
                });
            }
        }

        void validateConfig(IRabbitMqConfig config)
        {
            if (config == null)
                throw new ArgumentNullException("config", "Configuration cannot be null");

            if (string.IsNullOrWhiteSpace(config.QueueName) || string.IsNullOrWhiteSpace(config.Host)
                || string.IsNullOrWhiteSpace(config.Password) || string.IsNullOrWhiteSpace(config.User)
                || string.IsNullOrWhiteSpace(config.Vhost))
                throw new InvalidRabbitMqConfigurationException(
                    "One or more of the configuration value QueueName, Username, Password, VHost, Host information is either null or empty");
        }

        /// <summary>
        /// Builds service bus URL from the Queue name
        /// </summary>
        /// <returns>Uri of service bus</returns>
        Uri uri()
        {
            var paths = _config.Vhost.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var path = string.Join("/", paths.Concat(new[] { _config.QueueName }).ToArray());

            var builder = new UriBuilder("rabbitmq", _config.Host)
            {
                Path = path,
                Query = _config.Options ?? ""
            };

            return builder.Uri;
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                if (_endpointCache != null)
                {
                    _endpointCache.Dispose();
                    _endpointCache = null;
                }
            }
            _disposed = true;
        }
    }
}
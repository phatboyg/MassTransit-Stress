namespace MassTransit.StressConsole
{
    using Topshelf;


    public class SelectService :
        ServiceControl
    {
        readonly ServiceControl _actualService;

        public SelectService(ServiceControl actualService)
        {
            _actualService = actualService;
        }

        public bool Start(HostControl hostControl)
        {
            return _actualService.Start(hostControl);
        }

        public bool Stop(HostControl hostControl)
        {
            return _actualService.Stop(hostControl);
        }
    }
}
using PipServices3.Commons.Refer;

namespace PipServices3.Azure.Services
{
    public class DummyCommandableAzureFunctionService: CommandableAzureFunctionService
    {
        public DummyCommandableAzureFunctionService() : base("dummies")
        {
            _dependencyResolver.Put("controller", new Descriptor("pip-services-dummies", "controller", "default", "*", "*"));
        }
    }
}

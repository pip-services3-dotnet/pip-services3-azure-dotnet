using PipServices3.Commons.Refer;

namespace PipServices3.Azure.Containers
{
    public class DummyCommandableAzureFunction: CommandableAzureFunction
    {
        public DummyCommandableAzureFunction() : base("dummy", "Dummy Azure function")
        {
            this._dependencyResolver.Put("controller", new Descriptor("pip-services-dummies", "controller", "default", "*", "*"));
            this._factories.Add(new DummyFactory());
        }
    }
}

using PipServices3.Azure.Services;
using PipServices3.Commons.Refer;
using PipServices3.Components.Build;

namespace PipServices3.Azure
{
    public class DummyFactory: Factory
    {
        public static readonly Descriptor Descriptor = new Descriptor("pip-services-dummies", "factory", "default", "default", "1.0");
        public static readonly Descriptor ControllerDescriptor = new Descriptor("pip-services-dummies", "controller", "default", "*", "1.0");
        public static readonly Descriptor CloudFunctionServiceDescriptor = new Descriptor("pip-services-dummies", "service", "azure-function", "*", "1.0");
        public static readonly Descriptor CmdCloudFunctionServiceDescriptor = new Descriptor("pip-services-dummies", "service", "commandable-azure-function", "*", "1.0");
    
        public DummyFactory(): base()
        {
            this.RegisterAsType(DummyFactory.ControllerDescriptor, typeof(DummyController));
            this.RegisterAsType(DummyFactory.CloudFunctionServiceDescriptor, typeof(DummyAzureFunctionService));
            this.RegisterAsType(DummyFactory.CmdCloudFunctionServiceDescriptor, typeof(DummyCommandableAzureFunctionService));
        }
    }
}
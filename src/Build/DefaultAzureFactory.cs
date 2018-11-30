using PipServices3.Commons.Refer;
using PipServices3.Azure.Config;
using PipServices3.Azure.Count;
using PipServices3.Azure.Log;
using PipServices3.Azure.Queues;
using PipServices3.Components.Build;

namespace PipServices3.Azure.Build
{
    public class DefaultAzureFactory : Factory
    {
        public static Descriptor Descriptor = new Descriptor("pip-services", "factory", "azure", "default", "1.0");

        public static Descriptor StorageMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "storage-message-queue", "default", "1.0");
        // TODO: Not ready for .net core 2.0, see  https://github.com/Azure/azure-service-bus-dotnet/issues/65
        //public static Descriptor ServiceBusMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "servicebus-message-queue", "*", "1.0");
        //public static Descriptor ServiceBusMessageTopicDescriptor = new Descriptor("pip-services", "message-queue", "servicebus-message-topic", "*", "1.0");
        public static Descriptor KeyVaultConfigReaderDescriptor = new Descriptor("pip-services", "config-reader", "key-vault", "*", "1.0");
        public static Descriptor AppInsightsCountersDescriptor = new Descriptor("pip-services", "counters", "app-insights", "*", "1.0");
        public static Descriptor AppInsightsLoggerDescriptor = new Descriptor("pip-services", "logger", "app-insights", "*", "1.0");

        public DefaultAzureFactory()
        {
            RegisterAsType(StorageMessageQueueDescriptor, typeof(StorageMessageQueue));
            // TODO: Not ready for .net core 2.0, see  https://github.com/Azure/azure-service-bus-dotnet/issues/65
            //RegisterAsType(ServiceBusMessageQueueDescriptor, typeof(ServiceBusMessageQueue));
            //RegisterAsType(ServiceBusMessageTopicDescriptor, typeof(ServiceBusMessageTopic));
            RegisterAsType(KeyVaultConfigReaderDescriptor, typeof(KeyVaultConfigReader));
            RegisterAsType(AppInsightsCountersDescriptor, typeof(AppInsightsCounters));
            RegisterAsType(AppInsightsLoggerDescriptor, typeof(AppInsightsLogger));
        }
    }
}

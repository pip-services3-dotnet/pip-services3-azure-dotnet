using PipServices3.Commons.Refer;
using PipServices3.Azure.Config;
using PipServices3.Azure.Count;
using PipServices3.Azure.Log;
using PipServices3.Azure.Queues;
using PipServices3.Components.Build;
using PipServices3.Azure.Lock;
using PipServices3.Azure.Metrics;

namespace PipServices3.Azure.Build
{
    public class DefaultAzureFactory : Factory
    {
        public static Descriptor Descriptor = new Descriptor("pip-services", "factory", "azure", "default", "1.0");
        public static Descriptor Descriptor3 = new Descriptor("pip-services3", "factory", "azure", "default", "1.0");

        public static Descriptor StorageMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "storage-message-queue", "default", "1.0");
        public static Descriptor StorageMessageQueue3Descriptor = new Descriptor("pip-services3", "message-queue", "storage-message-queue", "default", "1.0");
        public static Descriptor ServiceBusMessageQueueDescriptor = new Descriptor("pip-services3", "message-queue", "servicebus-message-queue", "*", "1.0");
        public static Descriptor ServiceBusMessageTopicDescriptor = new Descriptor("pip-services3", "message-queue", "servicebus-message-topic", "*", "1.0");
        public static Descriptor KeyVaultConfigReaderDescriptor = new Descriptor("pip-services", "config-reader", "key-vault", "*", "1.0");
        public static Descriptor KeyVaultConfigReader3Descriptor = new Descriptor("pip-services3", "config-reader", "key-vault", "*", "1.0");
        public static Descriptor AppInsightsCountersDescriptor = new Descriptor("pip-services", "counters", "app-insights", "*", "1.0");
        public static Descriptor AppInsightsCounters3Descriptor = new Descriptor("pip-services3", "counters", "app-insights", "*", "1.0");
        public static Descriptor AppInsightsLoggerDescriptor = new Descriptor("pip-services", "logger", "app-insights", "*", "1.0");
        public static Descriptor AppInsightsLogger3Descriptor = new Descriptor("pip-services3", "logger", "app-insights", "*", "1.0");
        public static Descriptor CloudStorageTableLockDescriptor = new Descriptor("pip-services", "lock", "storage-table", "*", "1.0");
        public static Descriptor CloudStorageTableLock3Descriptor = new Descriptor("pip-services3", "lock", "storage-table", "*", "1.0");
        public static Descriptor CosmosDbMetricsServiceDescriptor = new Descriptor("pip-services", "metrics-service", "cosmosdb", "*", "1.0");
        public static Descriptor CosmosDbMetricsService3Descriptor = new Descriptor("pip-services3", "metrics-service", "cosmosdb", "*", "1.0");

        public DefaultAzureFactory()
        {
            RegisterAsType(StorageMessageQueueDescriptor, typeof(StorageMessageQueue));
            RegisterAsType(StorageMessageQueue3Descriptor, typeof(StorageMessageQueue));
            RegisterAsType(ServiceBusMessageQueueDescriptor, typeof(ServiceBusMessageQueue));
            RegisterAsType(ServiceBusMessageTopicDescriptor, typeof(ServiceBusMessageTopic));
            RegisterAsType(KeyVaultConfigReaderDescriptor, typeof(KeyVaultConfigReader));
            RegisterAsType(KeyVaultConfigReader3Descriptor, typeof(KeyVaultConfigReader));
            RegisterAsType(AppInsightsCountersDescriptor, typeof(AppInsightsCounters));
            RegisterAsType(AppInsightsCounters3Descriptor, typeof(AppInsightsCounters));
            RegisterAsType(AppInsightsLoggerDescriptor, typeof(AppInsightsLogger));
            RegisterAsType(AppInsightsLogger3Descriptor, typeof(AppInsightsLogger));
            RegisterAsType(CloudStorageTableLockDescriptor, typeof(CloudStorageTableLock));
            RegisterAsType(CloudStorageTableLock3Descriptor, typeof(CloudStorageTableLock));
            RegisterAsType(CosmosDbMetricsServiceDescriptor, typeof(CosmosDbMetricsService));
            RegisterAsType(CosmosDbMetricsService3Descriptor, typeof(CosmosDbMetricsService));
        }
    }
}

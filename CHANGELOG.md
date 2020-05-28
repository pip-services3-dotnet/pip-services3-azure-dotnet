# Azure specific components for Pip.Services in .NET Changelog

## <a name="3.1.0"></a> 3.1.0 (2020-05-26)

### Breaking Changes
* Migrated to .NET Core 3.1

## <a name="3.0.0-3.0.15"></a> 3.0.0-3.0.15 (2020-03-20)

### Features
* **queues** Add ServiceBusMessageTopic and ServiceBusMessageQueue
* **lock** Add CloudStorageLock
* **metrics** Add CosmosDbMetricsService
* **persistence** Add CosmosMongoDbPartitionPersistence
* **auth** Extended KeyVault client

### Breaking Changed
* Updated dependencies to version 3.0
* Added 'pip-services' descriptors

## <a name="2.1.0"></a> 2.1.0 (2017-05-17)

### Features
* Migrated to Service Fabric 2.6 and latest versions of WindowsAzure.ServiceBus and WindowsAzure.Storage

## <a name="2.0.0"></a> 2.0.0 (2017-02-27)

### Breaking Changes
* Migrated to **pip-services3** 2.0

## <a name="1.0.0"></a> 1.0.0 (2016-12-10)

Initial public release

### Features
* **auth** KeyVault client
* **config** Secure KeyVaultConfigReader
* **count** AppInsights counters
* **log** AppInsights logger
* **messaging** Azure Storage and Service Bus queues

### Bug Fixes
* Updated documentation
* Added checks for open state in queues
* Added dynamic creation of temporary subscriptions for Service Bus topics



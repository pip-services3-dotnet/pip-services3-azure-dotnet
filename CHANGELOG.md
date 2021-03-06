# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> Azure specific components .NET Changelog

## <a name="3.2.0-3.2.2"></a> 3.2.0-3.2.2 (2021-07-05) 

### Features
* Updated references as PipServices3.Components and PipServices3.Messaging have got minor changes

## <a name="3.1.6"></a> 3.1.6 (2021-05-12)

### Features
* Update PipServices3.MongoDb dependency

## <a name="3.1.5"></a> 3.1.5 (2020-11-10)

### Features
* Removed IsLocked method from CloudStorageTableLock

## <a name="3.1.4"></a> 3.1.4 (2020-11-10)

### Features
* Added IsLocked method to CloudStorageTableLock

## <a name="3.1.2-3.1.3"></a> 3.1.2-3.1.3 (2020-06-29)

### Features
* Implemented support backward compatibility
* Updated AppInsights telemetry configuration

## <a name="3.1.0-3.1.1"></a> 3.1.0-3.1.1 (2020-05-28)

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



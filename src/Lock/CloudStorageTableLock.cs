using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Refer;
using PipServices3.Components.Log;

using System;

using Azure;
using Azure.Data.Tables;

namespace PipServices3.Azure.Lock
{
    public class CloudStorageTableLock : Components.Lock.Lock, IConfigurable, IReferenceable
    {
        private TableServiceClient _serviceClient;
        private TableClient _tableClient;
        private ConfigParams _connectionConfigParams;

        protected CompositeLogger _logger = new CompositeLogger();

        public override void Configure(ConfigParams config)
        {
            _connectionConfigParams = ConfigParams.FromTuples(
                "Table", config.GetAsNullableString("table"),
                "DefaultEndpointsProtocol", config.GetAsNullableString("connection.protocol"),
                "AccountName", config.GetAsNullableString("credential.access_id"),
                "AccountKey", config.GetAsNullableString("credential.access_key"));
        }

        public void SetReferences(IReferences references)
        {
            _logger.SetReferences(references);

            InitializeConnection();
        }

        private void InitializeConnection()
        {
            try
            {
                var connection = _connectionConfigParams;

                var tableName = connection.Get("Table") ?? "locks";
                connection.Remove("Table");

                var newConnectionString = connection.ToString();

                _serviceClient = new TableServiceClient(newConnectionString);

                _tableClient = _serviceClient.GetTableClient(tableName);

                _logger.Trace("CloudStorageTableLock", $"Connected to lock storage table '{tableName}'.");
            }
            catch (Exception exception)
            {
                _logger.Error("CloudStorageTableLock", exception,
                    $"Failed to configure the connection to lock storage table with parameters '{_connectionConfigParams}'.");
            }
        }

        public override bool TryAcquireLock(string correlationId, string key, long timeToLive)
        {
            try
            {
                if (_tableClient == null)
                {
                    _logger.Error(correlationId, $"TryAcquireLock: Lock storage table is not initialized.");
                    return false;
                }

                var result = _tableClient.GetEntityIfExists<TableLock>(correlationId, key);

                if (result.HasValue)
                {
                    var tableLock = result.Value;

                    if (tableLock.Expired > DateTime.UtcNow)
                    {
                        _logger.Trace(correlationId, $"TryAcquireLock: Key = '{key}' has been already locked and not expired.");
                        return false;
                    }

                    _logger.Trace(correlationId, $"TryAcquireLock: Locked key = '{key}' expired.");
                }

                var lockRecord = new TableLock(correlationId, key, timeToLive);

                _tableClient.UpsertEntity(lockRecord);

                _logger.Trace(correlationId, $"TryAcquireLock: Set Key = '{key}' to 'lock' state; it will be expired at {lockRecord.Expired.ToString()} UTC.");
                return true;
            }
            catch (Exception exception)
            {
                _logger.Error(correlationId, exception, $"TryAcquireLock: Failed to acquire lock for key = '{key}'.");
                return false;
            }
        }

        public override void ReleaseLock(string correlationId, string key)
        {
            try
            {
                if (_tableClient == null)
                {
                    _logger.Error(correlationId, $"TryAcquireLock: Lock storage table is not initialized.");
                    return;
                }

                var result = _tableClient.GetEntityIfExists<TableLock>(correlationId, key);

                if (result.HasValue)
                {
                    try
                    {
                        _tableClient.DeleteEntity(correlationId, key);
                        _logger.Trace(correlationId, $"ReleaseLock: Key = '{key}' is released from 'lock' state.");
                    }
                    catch (Exception exception)
                    {
                        _logger.Error(correlationId, exception, $"ReleaseLock: Failed to release lock for key = '{key}'.");
                    }
                }
            }
            catch (Exception exception)
            {
                _logger.Error(correlationId, exception, $"ReleaseLock: Failed to release lock for key = '{key}'.");
            }
        }

        public class TableLock : ITableEntity
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public ETag ETag { get; set; }
            public DateTime Expired { get; set; }

            public TableLock() { }

            public TableLock(string correlationId, string key, long timeToLive)
            {
                PartitionKey = correlationId;
                RowKey = key;
                Timestamp = DateTimeConverter.ToDateTime(DateTime.UtcNow);
                Expired = DateTimeConverter.ToDateTime(DateTime.UtcNow.AddMilliseconds(timeToLive));
            }
        }
    }
}

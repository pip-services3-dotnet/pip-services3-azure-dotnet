using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Refer;
using PipServices3.Components.Log;

using System;

using Microsoft.Azure.Cosmos.Table;

namespace PipServices3.Azure.Lock
{
    public class CloudStorageTableLock : Components.Lock.Lock, IConfigurable, IReferenceable
    {
        private CloudTableClient _client;
        private CloudTable _table;
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
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(newConnectionString);

                _client = storageAccount.CreateCloudTableClient();

                _table = _client.GetTableReference(tableName.ToString());
                _table.CreateIfNotExistsAsync().Wait();

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
                if (_table == null)
                {
                    _logger.Error(correlationId, $"TryAcquireLock: Lock storage table is not initialized.");
                    return false;
                }

                var operation = TableOperation.Retrieve<TableLock>(correlationId, key);
                var record = _table.ExecuteAsync(operation).Result;
                var tableLock = record.Result as TableLock;

                if (tableLock != null)
                {
                    if (tableLock.Expired > DateTime.UtcNow)
                    {
                        _logger.Trace(correlationId, $"TryAcquireLock: Key = '{key}' has been already locked and not expired.");
                        return false;
                    }

                    _logger.Trace(correlationId, $"TryAcquireLock: Locked key = '{key}' expired.");
                }

                var lockRecord = new TableLock(correlationId, key, timeToLive);

                var insertOrReplaceOperation = TableOperation.InsertOrReplace(lockRecord);
                _table.ExecuteAsync(insertOrReplaceOperation).Wait();

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
                if (_table == null)
                {
                    _logger.Error(correlationId, $"TryAcquireLock: Lock storage table is not initialized.");
                    return;
                }

                var operation = TableOperation.Retrieve<TableLock>(correlationId, key);
                var result = _table.ExecuteAsync(operation).Result;
                if (result.Result != null)
                {
                    var record = (TableLock)(result.Result);
                    operation = TableOperation.Delete(record);
                    try
                    {
                        _table.ExecuteAsync(operation).Wait();
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

        public class TableLock : TableEntity
        {
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

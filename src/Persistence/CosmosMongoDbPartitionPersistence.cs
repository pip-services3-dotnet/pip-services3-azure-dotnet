using Microsoft.Azure.Documents;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using PipServices3.MongoDb.Persistence;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Data;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PipServices3.Azure.Persistence
{
    public abstract class CosmosMongoDbPartitionPersistence<T, K> : IdentifiableMongoDbPersistence<T, K>
        where T : IIdentifiable<K>
        where K : class
    {
        protected string _partitionKey;
        protected bool _cosmosDbApiEnabled = true;
        protected ICosmosDbRestClient _cosmosDbRestClient;

        public CosmosMongoDbPartitionPersistence(string collectionName, string partitionKey)
            : base(collectionName)
        {
            _partitionKey = partitionKey;
        }

        protected abstract string GetPartitionKey(K id);
        protected abstract List<string> GetIndexes();

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            _cosmosDbApiEnabled = config.GetAsBooleanWithDefault("cosmosdb_api_enabled", true);
        }

        public override async Task OpenAsync(string correlationId)
        {
            await base.OpenAsync(correlationId);

            if (!_cosmosDbApiEnabled)
            {
                _logger.Warn(correlationId, $"OpenAsync: Using CosmosDB API is disabled.");
                return;
            }

            InitializeCosmosDbRestClient();

            if (!await CollectionExistsAsync(correlationId))
            {
                await CreatePartitionCollectionAsync(correlationId);
            }
        }

        public override async Task<T> GetOneByIdAsync(string correlationId, K id)
        {
            var key = string.Empty;
            var filter = ComposePartitionFilter(id, out key);

            var result = await _collection.Find(filter).FirstOrDefaultAsync();

            if (result == null)
            {
                _logger.Trace(correlationId, $"Nothing found from {_collectionName} with id = '{id}' and {_partitionKey} = '{key}'.");
                return await Task.FromResult(default(T));
            }

            _logger.Trace(correlationId, $"Retrieved from {_collectionName} with id = '{id}' and {_partitionKey} = '{key}'.");
            return result;
        }

        public override async Task<object> GetOneByIdAsync(string correlationId, K id, ProjectionParams projection)
        {
            var key = string.Empty;
            var filter = ComposePartitionFilter(id, out key);

            var projectionBuilder = Builders<T>.Projection;
            var projectionDefinition = CreateProjectionDefinition(projection, projectionBuilder);

            var result = await _collection.Find(filter).Project(projectionDefinition).FirstOrDefaultAsync();

            if (result == null)
            {
                _logger.Trace(correlationId, $"Nothing found from {_collectionName} with id = '{id}' and {_partitionKey} = '{key}' and projection fields = '{StringConverter.ToString(projection)}'.");
                return null;
            }

            if (result.ElementCount == 0)
            {
                _logger.Trace(correlationId, $"Retrieved from {_collectionName} with id = '{id}' and {_partitionKey} = '{key}', but projection = '{StringConverter.ToString(projection)}' is not valid.");
                return null;
            }

            _logger.Trace(correlationId, $"Retrieved from {_collectionName} with id = '{id}' and {_partitionKey} = '{key}' and projection fields = '{StringConverter.ToString(projection)}'.");

            return BsonSerializer.Deserialize<object>(result);
        }

        public override async Task<T> DeleteByIdAsync(string correlationId, K id)
        {
            var key = string.Empty;
            var filter = ComposePartitionFilter(id, out key);

            var options = new FindOneAndDeleteOptions<T>();
            var result = await _collection.FindOneAndDeleteAsync(filter, options);

            _logger.Trace(correlationId, $"Deleted from {_collectionName} with id = {id} and {_partitionKey} = {key}");

            return result;
        }

        public override async Task<T> ModifyByIdAsync(string correlationId, K id, UpdateDefinition<T> updateDefinition)
        {
            if (id == null || updateDefinition == null)
            {
                return default(T);
            }

            var key = string.Empty;
            var filter = ComposePartitionFilter(id, out key);

            var result = await ModifyAsync(correlationId, filter, updateDefinition);

            _logger.Trace(correlationId, $"Modified in {_collectionName} with id = {id} and {_partitionKey} = {key}");

            return result;
        }

        public override async Task<T> UpdateAsync(string correlationId, T item)
        {
            var identifiable = item as IIdentifiable<K>;
            if (identifiable == null || item.Id == null)
            {
                return default(T);
            }

            var key = string.Empty;
            var filter = ComposePartitionFilter(identifiable.Id, out key);

            var options = new FindOneAndReplaceOptions<T>
            {
                ReturnDocument = ReturnDocument.After,
                IsUpsert = false
            };
            var result = await _collection.FindOneAndReplaceAsync(filter, item, options);

            _logger.Trace(correlationId, $"Updated in {_collectionName} with id = {identifiable.Id} and {_partitionKey} = {key}");

            return result;
        }

        public override async Task<T> SetAsync(string correlationId, T item)
        {
            var identifiable = item as IIdentifiable<K>;
            if (identifiable == null || item.Id == null)
            {
                return default(T);
            }

            var key = string.Empty;
            var filter = ComposePartitionFilter(identifiable.Id, out key);

            var options = new FindOneAndReplaceOptions<T>
            {
                ReturnDocument = ReturnDocument.After,
                IsUpsert = true
            };
            var result = await _collection.FindOneAndReplaceAsync(filter, item, options);

            _logger.Trace(correlationId, $"Set in {_collectionName} with id = {identifiable.Id} and {_partitionKey} = {key}");

            return result;
        }

        public async Task<T> GetOneByFilterAsync(string correlationId, FilterDefinition<T> filter)
        {
            var result = await _collection.Find(filter).FirstOrDefaultAsync();

            if (result == null)
            {
                _logger.Trace(correlationId, $"GetOneByFilter: nothing found in {_collectionName} with filter.");
                return default(T);
            }

            _logger.Trace(correlationId, $"GetOneByFilter: retrieved first element from {_collectionName} by filter.");

            return result;
        }

        protected virtual void InitializeCosmosDbRestClient()
        {
            _cosmosDbRestClient = new CosmosDbRestClient()
            {
                MongoClient = _connection,
                CollectionName = _collectionName,
                PartitionKey = _partitionKey,
                Logger = _logger
            };
        }

        protected virtual async Task CreatePartitionCollectionAsync(string correlationId)
        {
            try
            {
                // Specific CosmosDB command that creates partition collection (it raises exception for MongoDB)
                await _database.RunCommandAsync(new BsonDocumentCommand<BsonDocument>(new BsonDocument
                    {
                        {"shardCollection", $"{_database.DatabaseNamespace.DatabaseName}.{_collectionName}"},
                        {"key", new BsonDocument {{ _partitionKey, "hashed"}}}
                    }));

                _logger.Info(correlationId, $"CreatePartitionCollectionAsync: Created partition collection '{_collectionName}'.");
            }
            catch (Exception exception)
            {
                _logger.Error(correlationId, exception, $"CreatePartitionCollectionAsync: Failed to create partition collection.");
            }

            try
            {
                var indexModels = new List<CreateIndexModel<T>>();

                foreach (var index in GetIndexes())
                {
                    indexModels.Add(new CreateIndexModel<T>(Builders<T>.IndexKeys.Hashed(index)));
                }

                await _collection.Indexes.CreateManyAsync(indexModels).ConfigureAwait(false);

                _logger.Info(correlationId, $"CreatePartitionCollectionAsync: Created indexes for collection '{_collectionName}'.");
            }
            catch (Exception exception)
            {
                _logger.Error(correlationId, exception, $"CreatePartitionCollectionAsync: Failed to create indexes for collection '{_collectionName}'.");
            }

            _collection = _database.GetCollection<T>(_collectionName);
        }

        protected virtual async Task<bool> CollectionExistsAsync(string correlationId)
        {
            return await _cosmosDbRestClient.CollectionExistsAsync(correlationId);
        }

        protected virtual async Task<U> ExecuteWithRetriesAsync<U>(string correlationId, Func<Task<U>> invokeFunc, int maxRetries = 3)
        {
            for (var retry = 1; retry <= maxRetries; retry++)
            {
                try
                {
                    return await invokeFunc();
                }
                catch (MongoConnectionException mongoConnectionException)
                {
                    _logger.Error(correlationId, $"MongoConnectionException happened on {retry}/{maxRetries} attempt.");

                    if (retry >= maxRetries)
                    {
                        throw mongoConnectionException;
                    }

                    await Task.Delay(1000);
                }
                catch (DocumentClientException documentClientException)
                {
                    _logger.Error(correlationId, $"DocumentClientException happened on {retry}/{maxRetries} attempt with status code: '{documentClientException.StatusCode}' and retry after: '{documentClientException.RetryAfter}' ticks.");

                    if ((int)documentClientException.StatusCode != 429)
                    {
                        throw;
                    }

                    await Task.Delay(documentClientException.RetryAfter);
                }
                catch (AggregateException aggregateException)
                {
                    if (!(aggregateException.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    var documentClientException = (DocumentClientException)aggregateException.InnerException;

                    _logger.Error(correlationId, $"AggregateException happened on {retry}/{maxRetries} attempt with status code: '{documentClientException.StatusCode}' and retry after: '{documentClientException.RetryAfter}' ticks.");

                    if ((int)documentClientException.StatusCode != 429)
                    {
                        throw;
                    }

                    await Task.Delay(documentClientException.RetryAfter);
                }
                catch (Exception exception)
                {
                    throw exception;
                }
            }

            return await Task.FromResult(default(U));
        }

        protected virtual FilterDefinition<T> ComposePartitionFilter(K id, out string key)
        {
            key = GetPartitionKey(id);

            var builder = Builders<T>.Filter;
            var filter = builder.Empty;

            filter &= builder.Eq(x => x.Id, id);
            filter &= builder.Eq(_partitionKey, key);
            return filter;
        }
    }

}

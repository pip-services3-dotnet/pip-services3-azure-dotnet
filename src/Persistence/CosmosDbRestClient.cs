using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading.Tasks;

using MongoDB.Driver;

using PipServices3.Azure.Persistence.Data;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using PipServices3.Components.Log;

namespace PipServices3.Azure.Persistence
{
    internal class CosmosDbRestClient : ICosmosDbRestClient
    {
        public MongoClient MongoClient { get; set; }
        public string CollectionName { get; set; }
        public string PartitionKey { get; set; }
        public ILogger Logger { get; set; }

        private string DatabaseName => MongoClient?.Settings?.Credential?.Source;
        private string BaseUri => $"https://{MongoClient?.Settings?.Server?.Host}";
        private string MasterKey => MongoClient?.Settings?.Credential?.Password;

        public HttpClient _httpClient { get; set; }

        public CosmosDbRestClient()
        {
            _httpClient = new HttpClient();
        }

        public async Task<bool> CollectionExistsAsync(string correlationId)
        {
            try
            {
                var client = UpdateHttpClientWithHeader("GET", "colls", $"dbs/{DatabaseName}/colls/{CollectionName}");
                {
                    var uri = new Uri($"{BaseUri}/dbs/{DatabaseName}/colls/{CollectionName}");
                    var response = await client.GetAsync(uri);
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        Logger.Trace(correlationId, $"CollectionExistsAsync: Collection '{CollectionName}' exists.");
                        return await Task.FromResult(true);
                    }
                    else if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        Logger.Trace(correlationId, $"CollectionExistsAsync: Collection '{CollectionName}' doesn't exist.");
                        return await Task.FromResult(false);
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while checking that collection '{CollectionName}' already exists. Response Content: {responseContent}");
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.Error(correlationId, exception, $"CollectionExistsAsync: Failed to check that the collection '{CollectionName}' already exists in the CosmosDB database '{DatabaseName}'.");
                throw exception;
            }
        }

        public async Task CreatePartitionCollectionAsync(string correlationId, int throughput, List<string> indexes)
        {
            try
            {
                var client = UpdateHttpClientWithHeader("POST", "colls", $"dbs/{DatabaseName}");
                {
                    client.DefaultRequestHeaders.Add("x-ms-offer-throughput", throughput.ToString());

                    var uri = new Uri($"{BaseUri}/dbs/{DatabaseName}/colls");
                    var response = await client.PostAsJsonAsync(uri, CreateCollectionEntity(indexes));
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        Logger.Trace(correlationId, $"CreatePartitionCollectionAsync: Collection '{CollectionName}' created.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while creating collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.Error(correlationId, exception, $"CreatePartitionCollectionAsync: Failed to create the collection '{CollectionName}' in the CosmosDB database '{DatabaseName}'.");
                throw exception;
            }
        }

        public async Task<int> GetThroughputAsync(string correlationId)
        {
            var result = 0;
            try
            {
                // 1. Get collection
                CollectionEntity collectionEntity = null;

                var client = UpdateHttpClientWithHeader("GET", "colls", $"dbs/{DatabaseName}/colls/{CollectionName}");
                {
                    var uri = new Uri($"{BaseUri}/dbs/{DatabaseName}/colls/{CollectionName}");
                    var response = await client.GetAsync(uri);
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        collectionEntity = JsonConverter.FromJson<CollectionEntity>(responseContent);
                        if (collectionEntity == null)
                        {
                            throw new NotFoundException(correlationId, "NotFound", $"Unable to find collection '{CollectionName}'. Response Content: {responseContent}");
                        }
                        Logger.Trace(correlationId, $"GetThroughputAsync: Found collection '{CollectionName}'.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while getting info about collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }

                // 2. Retrieve offer of collection (throughput)
                OfferEntity offerEntity = null;
                client = UpdateHttpClientWithHeader("POST", "offers", "");
                {
                    client.DefaultRequestHeaders.Add("x-ms-documentdb-isquery", "True");

                    var uri = new Uri($"{BaseUri}/offers");
                    var value = new { query = $"SELECT * FROM root WHERE (root[\"offerResourceId\"] = \"{collectionEntity.ResourceId}\")" };
                    var response = await client.PostAsync(uri, value, new NoCharSetJsonMediaTypeFormatter());
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        var searchOffersEntity = JsonConverter.FromJson<SearchOffersEntity>(responseContent);
                        offerEntity = searchOffersEntity?.Offers?.FirstOrDefault();
                        if (offerEntity == null || offerEntity.Content == null)
                        {
                            throw new NotFoundException(correlationId, "NotFound", $"Unable to find offer of collection '{CollectionName}'. Response Content: {responseContent}");
                        }

                        result = offerEntity.Content.OfferThroughput;

                        Logger.Trace(correlationId, $"GetThroughputAsync: The current throughput of collection '{CollectionName}' is: '{result}'.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while getting offer of collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.Error(correlationId, exception, $"GetThroughputAsync: Failed to get throughput of the collection '{CollectionName}' in the CosmosDB database '{DatabaseName}'.");
                return await Task.FromResult(result);
            }

            return await Task.FromResult(result);
        }

        public async Task<bool> UpdateThroughputAsync(string correlationId, int throughput)
        {
            try
            {
                // 1. Get collection
                CollectionEntity collectionEntity = null;

                var client = UpdateHttpClientWithHeader("GET", "colls", $"dbs/{DatabaseName}/colls/{CollectionName}");
                {
                    var uri = new Uri($"{BaseUri}/dbs/{DatabaseName}/colls/{CollectionName}");
                    var response = await client.GetAsync(uri);
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        collectionEntity = JsonConverter.FromJson<CollectionEntity>(responseContent);
                        if (collectionEntity == null)
                        {
                            throw new NotFoundException(correlationId, "NotFound", $"Unable to find collection '{CollectionName}'. Response Content: {responseContent}");
                        }
                        Logger.Trace(correlationId, $"UpdateThroughputAsync: Found collection '{CollectionName}'.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while getting info about collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }

                // 2. Retrieve offer of collection (throughput)
                OfferEntity offerEntity = null;
                client = UpdateHttpClientWithHeader("POST", "offers", "");
                {
                    client.DefaultRequestHeaders.Add("x-ms-documentdb-isquery", "True");

                    var uri = new Uri($"{BaseUri}/offers");
                    var value = new { query = $"SELECT * FROM root WHERE (root[\"offerResourceId\"] = \"{collectionEntity.ResourceId}\")" };
                    var response = await client.PostAsync(uri, value, new NoCharSetJsonMediaTypeFormatter());
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        var searchOffersEntity = JsonConverter.FromJson<SearchOffersEntity>(responseContent);
                        offerEntity = searchOffersEntity?.Offers?.FirstOrDefault();
                        if (offerEntity == null || offerEntity.Content == null)
                        {
                            throw new NotFoundException(correlationId, "NotFound", $"Unable to find offer of collection '{CollectionName}'. Response Content: {responseContent}");
                        }

                        Logger.Trace(correlationId, $"UpdateThroughputAsync: The current throughput of collection '{CollectionName}' is: '{offerEntity?.Content?.OfferThroughput}'.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while getting offer of collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }

                // 3. Verify existing throughput
                if (throughput.Equals(offerEntity.Content.OfferThroughput))
                {
                    Logger.Trace(correlationId, $"UpdateThroughputAsync: Skip to update throughput of collection '{CollectionName}' with the same throughput: '{throughput}'.");
                    return await Task.FromResult(true);
                }

                // 4. Update offer (new throughput)
                offerEntity.Content.OfferThroughput = throughput;
                client = UpdateHttpClientWithHeader("PUT", "offers", $"{offerEntity.Id.ToLower()}");
                {
                    var uri = new Uri($"{BaseUri}/offers/{offerEntity.Id.ToLower()}");
                    var response = await client.PutAsJsonAsync(uri, offerEntity);
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        var resultOfferEntity = JsonConverter.FromJson<OfferEntity>(responseContent);
                        Logger.Trace(correlationId, $"UpdateThroughputAsync: The updated throughput of collection '{CollectionName}' is: '{resultOfferEntity?.Content?.OfferThroughput}'.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while updating throughput of collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.Error(correlationId, exception, $"UpdateThroughputAsync: Failed to update throughput to '{throughput}' of the collection '{CollectionName}' in the CosmosDB database '{DatabaseName}'.");
                return await Task.FromResult(false);
            }

            return await Task.FromResult(true);
        }

        public async Task UpdatePartitionCollectionAsync(string correlationId, List<string> indexes)
        {
            try
            {
                // 1. Get collection
                CollectionEntity collectionEntity = null;

                var client = UpdateHttpClientWithHeader("GET", "colls", $"dbs/{DatabaseName}/colls/{CollectionName}");
                {
                    var uri = new Uri($"{BaseUri}/dbs/{DatabaseName}/colls/{CollectionName}");
                    var response = await client.GetAsync(uri);
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        collectionEntity = JsonConverter.FromJson<CollectionEntity>(responseContent);
                        if (collectionEntity == null)
                        {
                            throw new NotFoundException(correlationId, "NotFound", $"Unable to find collection '{CollectionName}'. Response Content: {responseContent}");
                        }
                        Logger.Trace(correlationId, $"UpdatePartitionCollectionAsync: Found collection '{CollectionName}'.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while updating collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }

                // 2. Update collection
                client = UpdateHttpClientWithHeader("PUT", "colls", $"dbs/{DatabaseName}/colls/{CollectionName}");
                {
                    var uri = new Uri($"{BaseUri}/dbs/{DatabaseName}/colls/{CollectionName}");
                    var response = await client.PutAsJsonAsync(uri, CreateCollectionEntity(collectionEntity, indexes));
                    var responseContent = await response.Content.ReadAsStringAsync();

                    if (response.IsSuccessStatusCode)
                    {
                        Logger.Trace(correlationId, $"UpdatePartitionCollectionAsync: Collection '{CollectionName}' updated.");
                    }
                    else
                    {
                        throw new ConnectionException(correlationId, "ConnectFailed", $"Error while updating collection '{CollectionName}'. Response Content: {responseContent}");
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.Error(correlationId, exception, $"UpdatePartitionCollectionAsync: Failed to update the collection '{CollectionName}' in the CosmosDB database '{DatabaseName}'.");
                throw exception;
            }
        }

        #region Helper Methods

        private CollectionEntity CreateCollectionEntity(List<string> indexes)
        {
            var result = new CollectionEntity()
            {
                Id = CollectionName,
                IndexingPolicy = new IndexingPolicyEntity()
                {
                    IndexingMode = "consistent",
                    Automatic = true,
                    IncludedPaths = new List<IncludedPathEntity>()
                    {
                        new IncludedPathEntity()
                        {
                            Path = "/*",
                            Indexes = new List<IndexEntity>()
                            {
                                new IndexEntity()
                                {
                                    Kind = "Range",
                                    DataType = "Number",
                                    Precision = -1
                                },
                                new IndexEntity()
                                {
                                    Kind = "Range",
                                    DataType = "String",
                                    Precision = -1
                                }
                            }
                        }
                    }
                },
                PartitionKey = new PartitionKeyEntity()
                {
                    Version = 1,
                    Kind = "Hash",
                    Paths = new List<string> { $"/'$v'/{PartitionKey}/'$v'" }
                }
            };

            indexes.ForEach(index =>
            {
                // ignore system "id" index
                if (!string.IsNullOrWhiteSpace(index) && !index.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                {
                    result.IndexingPolicy.IncludedPaths.Add(new IncludedPathEntity()
                    {
                        Path = $"/{index}/?",
                        Indexes = new List<IndexEntity>()
                        {
                            new IndexEntity()
                            {
                                Kind = "Hash",
                                DataType = "Number",
                                Precision = -1
                            },
                            new IndexEntity()
                            {
                                Kind = "Hash",
                                DataType = "String",
                                Precision = -1
                            }
                        }
                    });
                }
            });

            return result;
        }

        private static CollectionEntity CreateCollectionEntity(CollectionEntity collectionEntity, List<string> indexes)
        {
            var result = new CollectionEntity()
            {
                Id = collectionEntity.Id,
                IndexingPolicy = new IndexingPolicyEntity()
                {
                    IndexingMode = "consistent",
                    Automatic = true,
                    IncludedPaths = new List<IncludedPathEntity>()
                    {
                        new IncludedPathEntity()
                        {
                            Path = "/*",
                            Indexes = new List<IndexEntity>()
                            {
                                new IndexEntity()
                                {
                                    Kind = "Range",
                                    DataType = "Number",
                                    Precision = -1
                                },
                                new IndexEntity()
                                {
                                    Kind = "Range",
                                    DataType = "String",
                                    Precision = -1
                                }
                            }
                        }
                    },
                    ExcludedPaths = collectionEntity.IndexingPolicy.ExcludedPaths
                },
                PartitionKey = collectionEntity.PartitionKey
            };

            foreach (var index in indexes)
            {
                // ignore system "id" index
                if (string.IsNullOrWhiteSpace(index) || index.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                {
                    continue;
                }

                result.IndexingPolicy.IncludedPaths.Add(new IncludedPathEntity()
                {
                    Path = $"/{index}/?",
                    Indexes = new List<IndexEntity>()
                        {
                            new IndexEntity()
                            {
                                Kind = "Hash",
                                DataType = "Number",
                                Precision = -1
                            },
                            new IndexEntity()
                            {
                                Kind = "Hash",
                                DataType = "String",
                                Precision = -1
                            }
                        }
                });
            }

            result.PartitionKey.Version = 1;

            return result;
        }

        private HttpClient UpdateHttpClientWithHeader(string verb, string resourceType, string resourceId)
        {
            var utcNow = DateTime.UtcNow.ToString("r");

            _httpClient.DefaultRequestHeaders.Clear();
            _httpClient.DefaultRequestHeaders.Add("x-ms-date", utcNow);
            _httpClient.DefaultRequestHeaders.Add("x-ms-version", "2015-12-16");

            var authHeader = CosmosDbAuthHelper.GenerateMasterKeyAuthorizationSignature(verb, resourceId, resourceType, MasterKey, "master", "1.0", utcNow);

            _httpClient.DefaultRequestHeaders.Add("authorization", authHeader);

            return _httpClient;
        }

        #endregion
    }

    //This is used when executing a query via REST
    //DocumentDB expects a specific Content-Type for queries
    //When setting the Content-Type header it must not have a charset, currently. 
    //This custom class sets the correct Content-Type and clears the charset
    public class NoCharSetJsonMediaTypeFormatter : JsonMediaTypeFormatter
    {
        public override void SetDefaultContentHeaders(Type type, System.Net.Http.Headers.HttpContentHeaders headers, System.Net.Http.Headers.MediaTypeHeaderValue mediaType)
        {
            base.SetDefaultContentHeaders(type, headers, new System.Net.Http.Headers.MediaTypeHeaderValue("application/query+json"));
            headers.ContentType.CharSet = "";
        }
    }
}
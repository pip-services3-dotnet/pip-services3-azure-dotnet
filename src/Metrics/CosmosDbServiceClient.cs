using Microsoft.Rest;

using System.Net.Http;
using System.Threading.Tasks;
using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;

using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace PipServices3.Azure.Metrics
{
    public class CosmosDbServiceClient : ServiceClient<CosmosDbServiceClient>
    {
        private Dictionary<ValueTuple<string, string>, ValueTuple<string, string>> _resourceMap = 
            new Dictionary<ValueTuple<string, string>, ValueTuple<string, string>>();

        public CosmosDbServiceClient(ServiceClientCredentials credentials)
        {
            InitializeHttpClient(new CosmosDbHttpClientHandler(credentials));
        }

        protected StringBuilder CreateBaseRequestUri(string resourceUri, string action, string apiVersion)
        {
            var requestUri = CreateResourceRequestUri(resourceUri);
            requestUri.Append($"{action}?api-version={apiVersion}");
            return requestUri;
        }

        protected StringBuilder CreateResourceRequestUri(string resourceUri)
        {
            var requestUri = new StringBuilder("https://management.azure.com");
            if (!resourceUri.StartsWith("/"))
            {
                requestUri.Append("/");
            }

            requestUri.Append(resourceUri);

            if (!resourceUri.EndsWith("/"))
            {
                requestUri.Append("/");
            }

            return requestUri;
        }

        public (string databaseResourceId, string collectionResourceId) GetResourceIDs(
            string documentDBAccount, 
            string documentDBKey, 
            string documentDBName, 
            string documentDBCollectionName)
        {
            string databaseResourceId = null;
            string collectionResourceId = null;

            var resourceKey = ValueTuple.Create(documentDBName, documentDBCollectionName);

            if (_resourceMap.ContainsKey(resourceKey))
            {
                return _resourceMap[resourceKey];
            }

            using (var client = new DocumentClient(new Uri($"https://{documentDBAccount}.documents.azure.com:443/"), documentDBKey))
            {
                var database = FindDocumentDBDatabaseByName(documentDBName, client);
                if (database == null)
                {
                    return (databaseResourceId, collectionResourceId);
                }

                databaseResourceId = database.ResourceId;

                if (!string.IsNullOrEmpty(documentDBCollectionName))
                {
                    var collection = FindDocumentDBCollectionByName(documentDBCollectionName, client, database);

                    if (!string.IsNullOrEmpty(collection.ResourceId))
                    {
                        collectionResourceId = collection.ResourceId;
                    }
                }

                _resourceMap[resourceKey] = ValueTuple.Create(databaseResourceId, collectionResourceId);

                return (databaseResourceId, collectionResourceId);
            }
        }

        private DocumentCollection FindDocumentDBCollectionByName(string documentDBCollectionName, DocumentClient client, Database database)
        {
            return client.CreateDocumentCollectionQuery(database.SelfLink)
                .Where(c => c.Id == documentDBCollectionName)
                .AsEnumerable()
                .FirstOrDefault();
        }

        private Database FindDocumentDBDatabaseByName(string documnetDBName, DocumentClient client)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == documnetDBName)
                .AsEnumerable()
                .FirstOrDefault();
        }

        public async Task<string> GetMetricsAsync(string resourceUri, string apiVersion, Action<QueryBuilder> queryBuilderDelegate)
        {
            var requestUri = CreateBaseRequestUri(resourceUri, "partitions/metrics", apiVersion);
            var queryBuilder = new QueryBuilder();
            queryBuilderDelegate(queryBuilder);

            var dataQuery = queryBuilder.ToString();
            if (!string.IsNullOrEmpty(dataQuery))
            {
                requestUri.Append("&$filter=");
                requestUri.Append(dataQuery);
            }

            var response = await HttpClient.GetAsync(requestUri.ToString());

            if (response.IsSuccessStatusCode)
            {
                return await response.Content.ReadAsStringAsync();
            }
            else
            {
                throw new Exception(response.ReasonPhrase);
            }
        }
    }
}

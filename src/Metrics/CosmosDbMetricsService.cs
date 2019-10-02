using PipServices3.Commons.Convert;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Config;
using PipServices3.Components.Log;
using PipServices3.Azure.Metrics.Data;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text;

namespace PipServices3.Azure.Metrics
{
    public class CosmosDbMetricsService : IReferenceable, IConfigurable, ICosmosDbMetricsService
    {
        private string ClientId { get; set; }
        private string ClientSecret { get; set; }
        private string SubscriptionId { get; set; }
        private string TenantId { get; set; }
        private string ApiVersion { get; set; }
        private CosmosDbClientCredentials ClientCredentials { get; set; }
        public CosmosDbServiceClient ServiceClient { get; set; }

        private const string DocumentDBCollectionUriFormat = "subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.DocumentDB/databaseAccounts/{2}/databases/{3}/collections/{4}";

        private CompositeLogger _logger = new CompositeLogger();

        public void Configure(ConfigParams config)
        {
            ClientId = config.GetAsString("credential.client_id");
            ClientSecret = ExtractClientSecret(config.GetAsString("credential.client_secret"));
            SubscriptionId = config.GetAsString("credential.subscription_id");
            TenantId = config.GetAsString("credential.tenant_id");
            ApiVersion = config.GetAsString("credential.api_version");
        }

        public void SetReferences(IReferences references)
        {
            ClientCredentials = new CosmosDbClientCredentials(ClientId, ClientSecret, TenantId);
            ServiceClient = new CosmosDbServiceClient(ClientCredentials);

            _logger.SetReferences(references);
        }

        public string GetResourceUri(string correlationId, string resourceGroupName, string accountName, string accessKey, string databaseName, string collectionName)
        {
            try
            {
                var (databaseResourceId, collectionResourceId) = ServiceClient.GetResourceIDs(accountName, accessKey, databaseName, collectionName);
                return string.Format(DocumentDBCollectionUriFormat, SubscriptionId, resourceGroupName, accountName, databaseResourceId, collectionResourceId);
            }
            catch (Exception exception)
            {
                _logger.Error(correlationId, exception, $"GetResourceUri: Failed to get uri for parameters: resourceGroupName = '{resourceGroupName}', " +
                    $"accountName = '{accountName}', accessKey = '{accessKey}, databaseName = '{databaseName}', collectionName = '{collectionName}'.");
                return string.Empty;
            }
        }

        public async Task<IEnumerable<Metric>> GetResourceMetricsAsync(string correlationId, string resourceUri, Action<QueryBuilder> queryBuilderDelegate)
        {
            try
            {
                var jsonMetrics = await ServiceClient.GetMetricsAsync(resourceUri, ApiVersion, 
                    queryBuilder => queryBuilderDelegate.Invoke(queryBuilder));

                var metrics = JsonConverter.FromJson<MetricCollection>(jsonMetrics);
                return metrics.Value;
            }
            catch (Exception exception)
            {
                _logger.Error(correlationId, exception, $"GetResourceMetricsAsync: Failed to get metrics for resource '{resourceUri}'.");
                return null;
            }
        }

        private string ExtractClientSecret(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return Encoding.UTF8.GetString(Convert.FromBase64String(value));
        }
    }
}

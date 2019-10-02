using PipServices3.Azure.Metrics.Data;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PipServices3.Azure.Metrics
{
    public interface ICosmosDbMetricsService
    {
        string GetResourceUri(string correlationId, string resourceGroupName, string accountName, string accessKey, string databaseName, string collectionName);
        Task<IEnumerable<Metric>> GetResourceMetricsAsync(string correlationId, string resourceUri, Action<QueryBuilder> oDataQueryBuilderDelegate);
    }
}
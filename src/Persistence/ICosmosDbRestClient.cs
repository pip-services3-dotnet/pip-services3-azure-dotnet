using System.Collections.Generic;
using System.Threading.Tasks;

namespace PipServices3.Azure.Persistence
{
    public interface ICosmosDbRestClient
    {
        Task<bool> CollectionExistsAsync(string correlationId);
        Task CreatePartitionCollectionAsync(string correlationId, int throughput, List<string> indexes);
        Task<int> GetThroughputAsync(string correlationId);
        Task<bool> UpdateThroughputAsync(string correlationId, int throughput);
        Task UpdatePartitionCollectionAsync(string correlationId, List<string> indexes);
    }
}
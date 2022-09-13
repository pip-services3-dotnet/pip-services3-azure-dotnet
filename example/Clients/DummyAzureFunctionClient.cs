using PipServices3.Commons.Data;
using System.Threading.Tasks;

namespace PipServices3.Azure.Clients
{
    public class DummyAzureFunctionClient : AzureFunctionClient, IDummyClient
    {
        public async Task<DataPage<Dummy>> GetDummiesAsync(string correlationId, FilterParams filter, PagingParams paging)
        {
            var response = await CallAsync<DataPage<Dummy>>("dummies.get_dummies", correlationId, new { filter, paging });

            return response;
        }

        public async Task<Dummy> CreateDummyAsync(string correlationId, Dummy dummy)
        {
            var response = await CallAsync<Dummy>("dummies.create_dummy", correlationId, new { dummy });

            return response;
        }

        public async Task<Dummy> GetDummyByIdAsync(string correlationId, string dummyId)
        {
            var response = await CallAsync<Dummy>("dummies.get_dummy_by_id", correlationId, new { dummy_id = dummyId });

            if (response == null)
                return null;

            return response;
        }

        public async Task<Dummy> UpdateDummyAsync(string correlationId, Dummy dummy)
        {
            var response = await CallAsync<Dummy>("dummies.update_dummy", correlationId, new { dummy = dummy });

            return response as Dummy;
        }

        public async Task<Dummy> DeleteDummyAsync(string correlationId, string dummyId)
        {
            var response = await CallAsync<Dummy>("dummies.delete_dummy", correlationId, new { dummy_id = dummyId });

            return response;
        }
    }
}

using PipServices3.Commons.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipServices3.Azure.Clients
{
    public class DummyCommandableAzureFunctionClient : CommandableAzureFunctionClient, IDummyClient
    {
        public DummyCommandableAzureFunctionClient() : base("dummies")
        {

        }

        public async Task<DataPage<Dummy>> GetDummiesAsync(string correlationId, FilterParams filter, PagingParams paging)
        {
            return await CallAsync<DataPage<Dummy>>("dummies.get_dummies", correlationId, new { filter, paging });
        }

        public async Task<Dummy> GetDummyByIdAsync(string correlationId, string dummyId)
        {
            var response = await this.CallAsync<Dummy>("dummies.get_dummy_by_id", correlationId, new { dummy_id = dummyId });

            if (response == null)
                return null;

            return response;
        }

        public async Task<Dummy> CreateDummyAsync(string correlationId, Dummy dummy)
        {
            return await CallAsync<Dummy>("dummies.create_dummy", correlationId, new { dummy });
        }

        public async Task<Dummy> UpdateDummyAsync(string correlationId, Dummy dummy)
        {
            return await this.CallAsync<Dummy>("dummies.update_dummy", correlationId, new { dummy });
        }

        public async Task<Dummy> DeleteDummyAsync(string correlationId, string dummyId)
        {
            return await CallAsync<Dummy>("dummies.delete_dummy", correlationId, new { dummy_id = dummyId });
        }
    }
}

using PipServices3.Commons.Data;
using System.Threading.Tasks;

namespace PipServices3.Azure
{
    public interface IDummyClient
    {
        Task<DataPage<Dummy>>  GetDummiesAsync(string correlationId, FilterParams filter, PagingParams paging);
        Task<Dummy>  GetDummyByIdAsync(string correlationId, string dummyId);
        Task<Dummy>  CreateDummyAsync(string correlationId, Dummy dummy);
        Task<Dummy>  UpdateDummyAsync(string correlationId, Dummy dummy);
        Task<Dummy>  DeleteDummyAsync(string correlationId, string dummyId);
    }
}
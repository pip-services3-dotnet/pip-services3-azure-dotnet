using System.Threading.Tasks;

using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;

namespace PipServices3.Azure.Connect
{
    public class AzureStorageConnectionResolver : IConfigurable, IReferenceable
    {
        protected ConnectionResolver _connectionResolver = new ConnectionResolver();
        
        protected CredentialResolver _credentialResolver = new CredentialResolver();
        
        public void Configure(ConfigParams config)
        {
            _connectionResolver.Configure(config);
            _credentialResolver.Configure(config);
        }
        
        public void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _credentialResolver.SetReferences(references);
        }
        
        public async Task<AzureStorageConnectionParams> ResolveAsync(string correlationId)
        {
            var result = new AzureStorageConnectionParams();

            var connection = await _connectionResolver.ResolveAsync(correlationId);
            result.Append(connection);

            var credential = await _credentialResolver.LookupAsync(correlationId);
            result.Append(credential);

            // Perform validation
            var err = result.Validate(correlationId);
            if (err != null)
            {
                throw err;
            }

            return result;
        }
    }
}
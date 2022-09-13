#if NETCOREAPP3_1_OR_GREATER

using Microsoft.Azure.Amqp.Framing;
using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace PipServices3.Azure.Connect
{
    public class AzureFunctionConnectionResolver : IConfigurable, IReferenceable
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

        public async Task<AzureFunctionConnectionParams> ResolveAsync(string correlationId)
        {
            var connection = new AzureFunctionConnectionParams();

            var connectionParams = await this._connectionResolver.ResolveAsync(correlationId);
            connection.Append(connectionParams);

            var credentialParams = await this._credentialResolver.LookupAsync(correlationId);
            connection.Append(credentialParams);

            // Perform validation
            connection.Validate(correlationId);

            connection = ComposeConnection(connection);

            return connection;
        }

        private AzureFunctionConnectionParams ComposeConnection(AzureFunctionConnectionParams connection)
        {
            connection = AzureFunctionConnectionParams.MergeConfigs(connection);

            var uri = connection.FunctionUri;

            if (uri == null || uri == "")
            {
                var protocol = connection.Protocol;
                var appName = connection.AppName;
                var functionName = connection.FunctionName;
                // http://myapp.azurewebsites.net/api/myfunction
                uri = $"{protocol}://{appName}.azurewebsites.net/api/{functionName}";

            connection.FunctionUri = uri;
            }
            else
            {
                var address = new Uri(uri);
                var protocol = address.Scheme;
                var appName = address.Host.Replace(".azurewebsites.net", "");
                var functionName = address.AbsolutePath.Replace("/api/", "");

                connection.Protocol = protocol;
                connection.AppName = appName;
                connection.FunctionName = functionName;
            }

            return connection;
        }
    }
}

#endif
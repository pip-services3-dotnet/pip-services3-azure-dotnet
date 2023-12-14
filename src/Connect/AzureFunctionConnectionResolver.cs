using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;

using System;
using System.Threading.Tasks;

namespace PipServices3.Azure.Connect
{
    /// <summary>
    /// Helper class used to retrieve Azure connection and credential parameters,
    /// validate them and compose a AzureFunctionConnectionParams value.
    /// 
    /// ### Configuration parameters ###
    /// 
    /// - connections:                   
    ///      - uri:           full connection uri with specific app and function name
    ///      - protocol:      connection protocol
    ///      - project_id:    is your Google Cloud Platform project ID
    ///      - region:        is the region where your function is deployed
    ///      - function_name: is the name of the HTTP function you deployed
    /// - credentials:    
    ///     - auth_token:    Google-generated ID token or null if using custom auth
    ///     
    /// ### References ###
    /// - :discovery:::1.0 - (optional) IDiscovery services to resolve connection.
    /// - :credential-store:*:*:1.0 - (optional) credential stores to resolve credentials.
    /// 
    /// <example>
    /// <code>
    /// var config = ConfigParams.FromTuples(
    ///     "connection.uri", "http://myapp.azurewebsites.net/api/myfunction",
    ///     "connection.app_name", "myapp",
    ///     "connection.function_name", "myfunction",
    ///     "credential.auth_code", "XXXXXXXXXX",
    /// );
    ///
    /// var connectionResolver = new AzureFunctionConnectionResolver();
    /// connectionResolver.Configure(config);
    /// connectionResolver.SetReferences(references);
    /// 
    /// var connectionParams = await connectionResolver.ResolveAsync("123");
    /// </code>
    /// </example>
    /// </summary>
    public class AzureFunctionConnectionResolver : IConfigurable, IReferenceable
    {
        /// <summary>
        /// Connection resolver.
        /// </summary>
        protected ConnectionResolver _connectionResolver = new ConnectionResolver();
        /// <summary>
        /// Credential resolver.
        /// </summary>
        protected CredentialResolver _credentialResolver = new CredentialResolver();

        /// <summary>
        /// Configures a component by passing its configuration parameters.
        /// </summary>
        /// <param name="config">configuration parameters to be set.</param>
        public void Configure(ConfigParams config)
        {
            _connectionResolver.Configure(config);
            _credentialResolver.Configure(config);
        }

        /// <summary>
        /// Sets references to dependent components.
        /// </summary>
        /// <param name="references">references to locate the component's dependencies.</param>
        public void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _credentialResolver.SetReferences(references);
        }

        /// <summary>
        /// Resolves connection and credential parameters and generates a single AzureFunctionConnectionParams value.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id used to trace execution through the call chain. </param>
        /// <returns>receives an AzureFunctionConnectionParams value or error.</returns>
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
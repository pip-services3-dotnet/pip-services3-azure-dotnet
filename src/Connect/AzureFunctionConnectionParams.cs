using PipServices3.Commons.Config;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;
using System.Collections.Generic;

namespace PipServices3.Azure.Connect
{
    /// <summary>
    /// Contains connection parameters to authenticate against Azure Functions
    /// and connect to specific Azure Function.
    /// The class is able to compose and parse Azure Function connection parameters.
    /// 
    /// In addition to standard parameters CredentialParams may contain any number of custom parameters
    /// 
    /// ### Configuration parameters ###
    /// - **connections**: 
    ///     - ** uri**:           full connection uri with specific app and function name
    ///     - ** protocol**:      connection protocol
    ///     - ** app_name**:      alternative app name
    ///     - ** function_name**: application function name
    /// - ** credentials**: 
    ///     - ** auth_code**:     authorization code or null if using custom auth
    /// 
    /// <example>
    /// <code>
    /// var connection = AzureFunctionConnectionParams.FromTuples(
    ///     "connection.uri", "http://myapp.azurewebsites.net/api/myfunction",
    ///     "connection.protocol", "http",
    ///     "connection.app_name", "myapp",
    ///     "connection.function_name", "myfunction",
    ///     "credential.auth_code", "code",
    /// );
    /// 
    /// var uri = connection.FunctionUri;             // Result: "http://myapp.azurewebsites.net/api/myfunction"
    /// var protocol = connection.AppName;            // Result: "http"
    /// var appName = connection.AppName;             // Result: "myapp"
    /// var functionName = connection.FunctionName;   // Result: "myfunction"
    /// var authCode = connection.getAuthCode;           // Result: "code"
    /// </code>
    /// </example>
    /// 
    /// 
    /// </summary>
    public class AzureFunctionConnectionParams : ConfigParams
    {
        /// <summary>
        /// Creates an new instance of the connection parameters.
        /// </summary>
        public AzureFunctionConnectionParams()
        { }

        /// <summary>
        /// Creates an new instance of the connection parameters.
        /// </summary>
        /// <param name="map">(optional) an object to be converted into key-value pairs to initialize this connection.</param>
        public AzureFunctionConnectionParams(IDictionary<string, string> map)
            : base(map)
        { }

        /// <summary>
        /// The Azure function connection protocol.
        /// </summary>
        public string Protocol
        {
            get => GetAsNullableString("protocol");
            set => Set("protocol", value);
        }
        /// <summary>
        /// The Azure function uri.
        /// </summary>
        public string FunctionUri
        {
            get => GetAsNullableString("uri");
            set => Set("uri", value);
        }
        /// <summary>
        /// The Azure function name.
        /// </summary>
        public string FunctionName
        {
            get => GetAsNullableString("function_name");
            set => Set("function_name", value);
        }
        /// <summary>
        /// The Azure app name.
        /// </summary>
        public string AppName
        {
            get => GetAsNullableString("app_name");
            set => Set("app_name", value);
        }
        /// <summary>
        /// The Azure auth code.
        /// </summary>
        public string AuthCode
        {
            get => GetAsNullableString("auth_code");
            set => Set("auth_code", value);
        }
        /// <summary>
        /// Validates this connection parameters 
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <exception cref="ConfigException"></exception>
        public void Validate(string correlationId)
        {
            var uri = FunctionUri;
            var protocol = Protocol;
            var appName = AppName;
            var functionName = FunctionName;

            if (string.IsNullOrEmpty(uri) && (string.IsNullOrEmpty(appName) && string.IsNullOrEmpty(functionName) && string.IsNullOrEmpty(protocol)))
            {
                throw new ConfigException(
                    correlationId,
                    "NO_CONNECTION_URI",
                    "No uri, app_name and function_name is configured in Auzre function uri"
                );
            }

            if (protocol != null && "http" != protocol && "https" != protocol)
            {
                throw new ConfigException(
                    correlationId, "WRONG_PROTOCOL", "Protocol is not supported by REST connection")
                    .WithDetails("protocol", protocol);
            }
        }

        public static new AzureFunctionConnectionParams FromString(string line)
        {
            var map = StringValueMap.FromString(line);
            return new AzureFunctionConnectionParams(map);
        }
        /// <summary>
        /// Validates this connection parameters 
        /// </summary>
        /// <param name="config">configuration parameters</param>
        /// <returns>the generated AzureFunctionConnectionParams object.</returns>
        public static AzureFunctionConnectionParams FromConfig(ConfigParams config)
        {
            var result = new AzureFunctionConnectionParams();

            var credentials = CredentialParams.ManyFromConfig(config);
            foreach (var credential in credentials)
                result.Append(credential);

            var connections = ConnectionParams.ManyFromConfig(config);
            foreach (var connection in connections)
                result.Append(connection);

            return result;
        }

        public static new AzureFunctionConnectionParams FromTuples(params object[] tuples)
        {
            var config = ConfigParams.FromTuples(tuples);
            return AzureFunctionConnectionParams.FromConfig(config);
        }
        /// <summary>
        /// Retrieves AzureFunctionConnectionParams from multiple configuration parameters.
        /// The values are retrieves from "connection" and "credential" sections.
        /// </summary>
        /// <param name="configs">a list with configuration parameters</param>
        /// <returns>the generated AzureFunctionConnectionParams object.</returns>
        public static AzureFunctionConnectionParams MergeConfigs(params ConfigParams[] configs)
        {
            var config = ConfigParams.MergeConfigs(configs);
            return new AzureFunctionConnectionParams(config);
        }
    }
}
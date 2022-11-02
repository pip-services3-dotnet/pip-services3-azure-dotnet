using PipServices3.Commons.Config;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;
using System.Collections.Generic;

namespace PipServices3.Azure.Connect
{
    public class AzureFunctionConnectionParams : ConfigParams
    {
        public AzureFunctionConnectionParams()
        { }

        public AzureFunctionConnectionParams(IDictionary<string, string> map)
            : base(map)
        { }

        public string Protocol
        {
            get => GetAsNullableString("protocol");
            set => Set("protocol", value);
        }
        public string FunctionUri
        {
            get => GetAsNullableString("uri");
            set => Set("uri", value);
        }

        public string FunctionName
        {
            get => GetAsNullableString("function_name");
            set => Set("function_name", value);
        }
        public string AppName
        {
            get => GetAsNullableString("app_name");
            set => Set("app_name", value);
        }
        public string AuthCode
        {
            get => GetAsNullableString("auth_code");
            set => Set("auth_code", value);
        }

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

        public static AzureFunctionConnectionParams MergeConfigs(params ConfigParams[] configs)
        {
            var config = ConfigParams.MergeConfigs(configs);
            return new AzureFunctionConnectionParams(config);
        }
    }
}
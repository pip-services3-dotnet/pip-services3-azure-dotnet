using System.Collections.Generic;

using PipServices3.Commons.Config;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;

namespace PipServices3.Azure.Connect
{
    public class AzureStorageConnectionParams : ConnectionParams
    {
        public AzureStorageConnectionParams()
        { }
        
        public AzureStorageConnectionParams(IDictionary<string, string> map)
            : base(map)
        { }
        
        public AzureStorageConnectionParams(ConnectionParams connection, CredentialParams credential)
        {
            if (connection != null)
            {
                Append(connection);
            }

            if (credential != null)
            {
                Append(credential);
            }
        }
        
        public static AzureStorageConnectionParams FromConfig(ConfigParams config)
        {
            var result = new AzureStorageConnectionParams();

            var credentials = CredentialParams.ManyFromConfig(config);
            foreach (var credential in credentials)
            {
                result.Append(credential);
            }

            var connections = ConnectionParams.ManyFromConfig(config);
            foreach (var connection in connections)
            {
                result.Append(connection);
            }

            return result;
        }
        
        public ConfigException Validate(string correlationId)
        {
            if (AccessId == null)
            {
                return new ConfigException(
                    correlationId,
                    "NO_ACCESS_ID",
                    "No access_id and account_name is configured in Azure Storage credential"
                );
            }

            if (AccessKey == null)
            {
                return new ConfigException(
                    correlationId,
                    "NO_ACCESS_KEY",
                    "No access_key and account_key is configured in Azure Storage credential"
                );
            }
            
            if (Container == null && Table == null)
            {
                return new ConfigException(
                    correlationId,
                    "NO_CONTAINER_AND_TABLE",
                    "No container and table is configured in Azure Storage credential"
                );
            }

            return null;
        }
        
        public string Container
        {
            get { return GetAsNullableString("container"); }
            set { base["container"] = value; }
        }
        
        public string Table
        {
            get { return GetAsNullableString("table"); }
            set { base["table"] = value; }
        }
        
        public new string Protocol
        {
            get { return GetAsNullableString("protocol") ?? GetAsNullableString("endpoints_protocol") ?? "https"; }
            set { base["protocol"] = value; }
        }
        
        public string Suffix
        {
            get { return GetAsNullableString("suffix") ?? GetAsNullableString("endpoints_suffix") ?? "core.windows.net"; }
            set { base["suffix"] = value; }
        }
        
        public string AccessId
        {
            get { return GetAsNullableString("access_id") ?? GetAsNullableString("account_name"); }
            set { base["access_id"] = value; }
        }
        
        public string AccessKey
        {
            get { return GetAsNullableString("access_key") ?? GetAsNullableString("account_key"); }
            set { base["access_key"] = value; }
        }
        
        public static new AzureStorageConnectionParams FromString(string line)
        {
            var map = StringValueMap.FromString(line);
            return new AzureStorageConnectionParams(map);
        }

        public override string ToString()
        {
            return
                $"DefaultEndpointsProtocol={Protocol};AccountName={AccessId};AccountKey={AccessKey};EndpointSuffix={Suffix}";
        }

        public static AzureStorageConnectionParams MergeConfigs(params ConfigParams[] configs)
        {
            var config = ConfigParams.MergeConfigs(configs);
            return new AzureStorageConnectionParams(config);
        }
    }
}
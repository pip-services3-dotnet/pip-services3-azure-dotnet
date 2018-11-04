using PipServices3.Azure.Auth;
using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Refer;
using System;

namespace PipServices3.Azure.Config
{
    /// <summary>
    /// Reads configuration from Azure KeyVault secrets. Secret key becomes a parameter name
    /// </summary>
    public class KeyVaultConfigReader : CachedConfigReader, IReferenceable, IConfigurable
    {
        private ConnectionResolver _connectionResolver = new ConnectionResolver();
        private CredentialResolver _credentialResolver = new CredentialResolver();

        public KeyVaultConfigReader() { }

        public KeyVaultConfigReader(ConfigParams config)
        {
            if (config != null) Configure(config);
        }

        public virtual void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _credentialResolver.SetReferences(references);
        }

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);
            _connectionResolver.Configure(config, true);
            _credentialResolver.Configure(config, true);
        }

        protected ConfigParams PerformReadConfig(string correlationId)
        {
            try
            {
                var connection = _connectionResolver.ResolveAsync(correlationId).Result;
                var credential = _credentialResolver.LookupAsync(correlationId).Result;
                KeyVaultClient _client = new KeyVaultClient(connection, credential);

                var secrets = _client.GetSecretsAsync().Result;
                var result = new ConfigParams();

                foreach (var entry in secrets)
                {
                    var key = entry.Key.Replace('-', '.');
                    var value = entry.Value;
                    result[key] = value;
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Failed to load config from KeyVault", ex);
            }
        }

        public static new ConfigParams ReadConfig(string correlationId, ConfigParams config)
        {
            return new KeyVaultConfigReader(config).PerformReadConfig(correlationId);
        }

        public static ConfigParams ReadConfig(string correlationId, string connectionString)
        {
            var config = ConfigParams.FromString(connectionString);
            return new KeyVaultConfigReader(config).PerformReadConfig(correlationId);
        }

        protected override ConfigParams PerformReadConfig(string correlationId, ConfigParams parameters)
        {
            return PerformReadConfig(correlationId);
        }
    }
}

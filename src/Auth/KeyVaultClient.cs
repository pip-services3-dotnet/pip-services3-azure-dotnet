using PipServices3.Components.Auth;
using PipServices3.Components.Connect;

using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

namespace PipServices3.Azure.Auth
{
    public class KeyVaultClient
    {
        private string _keyVault;
        private string _clientId;
        private string _clientKey;
        private string _thumbPrint;
        private string _tenantId;
        private SecretClient _client;

        public KeyVaultClient(ConnectionParams connection, CredentialParams credential)
        {
            _keyVault = connection.GetAsNullableString("key_vault") 
                ?? connection.GetAsNullableString("uri") 
                ?? connection.GetAsNullableString("KeyVault");
            if (_keyVault == null)
                throw new ArgumentNullException("KeyVault parameter is not defined");
            if (!_keyVault.StartsWith("http"))
                _keyVault = "https://" + _keyVault + ".vault.azure.net";

            _clientId = credential.AccessId ?? credential.GetAsNullableString("ClientId");
            if (_clientId == null)
                throw new ArgumentNullException("CliendId parameter is not defined");

            _clientKey = credential.AccessKey ?? credential.GetAsNullableString("ClientKey");
            _thumbPrint = credential.GetAsNullableString("thumbprint")
                ?? credential.GetAsNullableString("ThumbPrint");
            _tenantId = credential.GetAsNullableString("tenant_id")
                ?? credential.GetAsNullableString("TenantId")
                ?? credential.GetAsNullableString("tenant");

            if (_clientKey == null && _thumbPrint == null)
                throw new ArgumentNullException("Neither ClientKey or ThumbPrint parameters are not defined");

            if (_clientKey != null)
            {
                if (string.IsNullOrWhiteSpace(_tenantId))
                    throw new ArgumentNullException("TenantId parameter is not defined for client secret authentication");

                var credentialClient = new ClientSecretCredential(_tenantId, _clientId, _clientKey);
                _client = new SecretClient(new Uri(_keyVault), credentialClient);
            }
            else
            {
                if (string.IsNullOrWhiteSpace(_tenantId))
                    throw new ArgumentNullException("TenantId parameter is not defined for certificate authentication");

                var cert = FindCertificateByThumbprint(_thumbPrint);
                if (cert == null)
                    throw new ArgumentNullException("Certificate with the specified thumbprint was not found");

                var credentialClient = new ClientCertificateCredential(_tenantId, _clientId, cert);
                _client = new SecretClient(new Uri(_keyVault), credentialClient);
            }
        }

        public async Task<List<string>> GetSecretNamesAsync()
        {
            var result = new List<string>();
            await foreach (var prop in _client.GetPropertiesOfSecretsAsync())
            {
                if (!string.IsNullOrEmpty(prop.Name))
                    result.Add(prop.Name);
            }
            return result;
        }

        public async Task<string> GetSecretValueAsync(string secretName)
        {
            var response = await _client.GetSecretAsync(secretName);
            return response?.Value?.Value;
        }

        public async Task SetSecretValueAsync(string secretName, string secretValue)
        {
            await _client.SetSecretAsync(secretName, secretValue);
        }

        public async Task DeleteSecretAsync(string secretName)
        {
            await _client.StartDeleteSecretAsync(secretName);
        }

        public async Task<Dictionary<string, string>> GetSecretsAsync()
        {
            var secretNames = await GetSecretNamesAsync();
            var result = new Dictionary<string, string>();
            foreach (var secretName in secretNames)
            {
                var secretValue = await GetSecretValueAsync(secretName);
                result[secretName] = secretValue;
            }
            return result;
        }

        public static X509Certificate2 FindCertificateByThumbprint(string thumbPrint)
        {
            X509Store certStore = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            try
            {
                certStore.Open(OpenFlags.ReadOnly);
                X509Certificate2Collection certCollection = certStore.Certificates.Find(
                    X509FindType.FindByThumbprint,
                    thumbPrint,
                    false
                ); // Don't validate certs, since the test root isn't installed.
                if (certCollection == null || certCollection.Count == 0)
                    return null;
                return certCollection[0];
            }
            finally
            {
                certStore.Close();
            }
        }
    }
}

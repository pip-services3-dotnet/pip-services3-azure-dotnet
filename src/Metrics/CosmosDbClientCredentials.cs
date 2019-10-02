using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;

using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace PipServices3.Azure.Metrics
{
    public class CosmosDbClientCredentials : ServiceClientCredentials
    {
        private string _tenantId = null;
        private string _clientId = null;
        private string _clientSecret = null;
        private string _resource = "https://management.core.windows.net/";

        private AuthenticationResult LatestAuthenticationResult { get; set; }

        public CosmosDbClientCredentials(string clientId, string clientSecret, string tenantId)
        {
            _tenantId = tenantId;
            _clientId = clientId;
            _clientSecret = clientSecret;
        }

        public override async Task ProcessHttpRequestAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request == null)
            {
                throw new ArgumentNullException("request");
            }

            var bearerToken = await GetBearerTokenAsync();

            if (bearerToken == null)
            {
                throw new InvalidOperationException("Token cannot be null");
            }

            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            await base.ProcessHttpRequestAsync(request, cancellationToken);
        }

        private async Task<string> GetBearerTokenAsync()
        {
            if (LatestAuthenticationResult?.ExpiresOn > DateTime.UtcNow)
            {
                return await Task.FromResult(LatestAuthenticationResult.AccessToken);
            }

            LoggerCallbackHandler.UseDefaultLogging = false;
            var authenticationContext = new AuthenticationContext($"https://login.windows.net/{_tenantId}");
            var credential = new ClientCredential(_clientId, _clientSecret);

            LatestAuthenticationResult = await authenticationContext.AcquireTokenAsync(_resource, credential);

            if (LatestAuthenticationResult == null)
            {
                throw new InvalidOperationException("Failed to obtain the token");
            }

            return await Task.FromResult(LatestAuthenticationResult.AccessToken);
        }
    }
}

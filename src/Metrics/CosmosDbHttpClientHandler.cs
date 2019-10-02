using Microsoft.Rest;

using System.Net.Http;
using System.Threading.Tasks;
using System.Threading;

namespace PipServices3.Azure.Metrics
{
    public class CosmosDbHttpClientHandler : HttpClientHandler
    {
        ServiceClientCredentials _credentials;
        public CosmosDbHttpClientHandler(ServiceClientCredentials credentials)
        {
            _credentials = credentials;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await _credentials.ProcessHttpRequestAsync(request, cancellationToken);
            return await base.SendAsync(request, cancellationToken);
        }
    }
}

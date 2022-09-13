using PipServices3.Azure.Services;
using PipServices3.Commons.Config;
using System;
using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Clients
{
    public class DummyAzureFunctionClientTest : IDisposable
    {
        protected DummyAzureFunctionClient client;
        protected DummyClientFixture fixture;

        private bool skip = false;

        public DummyAzureFunctionClientTest()
        {
            var appName = Environment.GetEnvironmentVariable("AZURE_FUNCTION_APP_NAME");
            var functionName = Environment.GetEnvironmentVariable("AZURE_FUNCTION_NAME");
            var protocol = Environment.GetEnvironmentVariable("AZURE_FUNCTION_PROTOCOL");
            var authCode = Environment.GetEnvironmentVariable("AZURE_FUNCTION_AUTH_CODE");
            var uri = Environment.GetEnvironmentVariable("AZURE_FUNCTION_URI"); //?? "http://localhost:7071/api/Function";

            if (string.IsNullOrEmpty(uri) && (string.IsNullOrEmpty(appName) || string.IsNullOrEmpty(functionName) || string.IsNullOrEmpty(protocol) || string.IsNullOrEmpty(authCode)))
            {
                skip = true;
                return;
            }

            var config = ConfigParams.FromTuples(
                "connection.uri", uri,
                "connection.protocol", protocol,
                "connection.app_name", appName,
                "connection.function_name", functionName,
                "credential.auth_code", authCode
            );

            client = new DummyAzureFunctionClient();
            client.Configure(config);

            fixture = new DummyClientFixture(typeof(Function), client);

            client.OpenAsync(null).Wait();
        }

        [Fact]
        public async Task TestCrudOperations()
        {
            if (!skip)
                await fixture.TestCrudOperations();
        }

        public void Dispose()
        {
            if (!skip)
                client.CloseAsync(null).Wait();
        }
    }
}

using PipServices3.Commons.Config;
using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Connect
{
    public class AzureFunctionConnectionParamsTest
    {
        [Fact]
        public void TestEmptyConnection()
        {
            var connection = new AzureFunctionConnectionParams();
            Assert.Null(connection.FunctionUri);
            Assert.Null(connection.AppName);
            Assert.Null(connection.FunctionName);
            Assert.Null(connection.AuthCode);
            Assert.Null(connection.Protocol);
        }

        [Fact]
        public async Task TestComposeConfigAsync()
        {
            var config1 = ConfigParams.FromTuples(
                "connection.uri", "http://myapp.azurewebsites.net/api/myfunction",
                "credential.auth_code", "1234"
            );

            var config2 = ConfigParams.FromTuples(
                "connection.protocol", "http",
                "connection.app_name", "myapp",
                "connection.function_name", "myfunction",
                "credential.auth_code", "1234"

            );

            var resolver = new AzureFunctionConnectionResolver();
            resolver.Configure(config1);
            var connection = await resolver.ResolveAsync("");

            Assert.Equal("http://myapp.azurewebsites.net/api/myfunction", connection.FunctionUri);
            Assert.Equal("myapp", connection.AppName);
            Assert.Equal("http", connection.Protocol);
            Assert.Equal("myfunction", connection.FunctionName);
            Assert.Equal("1234", connection.AuthCode);

            resolver = new AzureFunctionConnectionResolver();
            resolver.Configure(config2);
            connection = await resolver.ResolveAsync("");

            Assert.Equal("http://myapp.azurewebsites.net/api/myfunction", connection.FunctionUri);
            Assert.Equal("http", connection.Protocol);
            Assert.Equal("myapp", connection.AppName);
            Assert.Equal("myfunction", connection.FunctionName);
            Assert.Equal("1234", connection.AuthCode);
        }
    }
}

using PipServices3.Commons.Config;
using Xunit;

namespace PipServices3.Azure.Connect
{
    public class AzureStorageConnectionParamsTest
    {
        [Fact]
        public void TestStorageConnectionParametersFromConfig()
        {
            var protocol = "test_protocol";
            var container = "test_container";
            var suffix = "test_suffix";
            var accountName = "test_account_name";
            var accountKey = "test_account_key";

            var config = ConfigParams.FromTuples(
                "connection.protocol", protocol,
                "connection.container", container,
                "connection.suffix", suffix,
                "credential.access_id", accountName,
                "credential.access_key", accountKey
            );

            var connectionParameters = AzureStorageConnectionParams.FromConfig(config);

            Assert.Equal(protocol, connectionParameters.Protocol);
            Assert.Equal(container, connectionParameters.Container);
            Assert.Equal(suffix, connectionParameters.Suffix);
            Assert.Equal(accountName, connectionParameters.AccessId);
            Assert.Equal(accountKey, connectionParameters.AccessKey);
        }
    }
}

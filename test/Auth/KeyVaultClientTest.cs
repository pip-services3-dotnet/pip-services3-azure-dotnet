//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using PipServices.Commons.Auth;
//using PipServices.Commons.Config;
//using PipServices.Commons.Connect;
//using System.Threading.Tasks;

//namespace PipServices.Azure.Auth
//{
//    [TestClass]
//    public class KeyVaultClientTest
//    {
//        [TestMethod]
//        public async Task TestReadingKeyVaultByClientIdAsync()
//        {
//            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml");
//            var connectionString = config.GetAsString("key_vault");
//            var connection = ConnectionParams.FromString(connectionString);
//            var credential = CredentialParams.FromString(connectionString);
//            var reader = new KeyVaultClient(connection, credential);
//            var secrets = await reader.GetSecretsAsync();
//            Assert.IsTrue(secrets.Count > 0);
//        }
//    }
//}

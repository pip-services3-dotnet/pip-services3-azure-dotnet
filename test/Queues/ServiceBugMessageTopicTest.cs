using Microsoft.VisualStudio.TestTools.UnitTesting;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
    [TestClass]
    public class ServiceBusMessageTopicTest
    {
        ServiceBusMessageTopic Topic { get; set; }
        MessageQueueFixture Fixture { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml");
            var connection = ConnectionParams.FromString(config.GetAsString("sb_topic"));
            Topic = new ServiceBusMessageTopic("TestTopic", connection);
            Topic.OpenAsync(null).Wait();
            Fixture = new MessageQueueFixture(Topic);
        }

        [TestMethod]
        public async Task TestSBTopicSendReceiveMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicReceiveSendMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicReceiveAndCompleteAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicReceiveAndAbandonAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicSendPeekMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicPeekNoMessageAsync()
        {
            await Topic.ClearAsync(null);
            //await Fixture.TestPeekNoMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicOnMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [TestMethod]
        public async Task TestSBTopicMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestSBTopicMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

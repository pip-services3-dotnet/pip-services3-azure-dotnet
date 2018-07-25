using Microsoft.VisualStudio.TestTools.UnitTesting;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
    [TestClass]
    public class ServiceBusMessageQueueTest
    {
        ServiceBusMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml");
            var connection = ConnectionParams.FromString(config.GetAsString("sb_queue"));
            Queue = new ServiceBusMessageQueue("TestQueue", connection);
            Queue.OpenAsync(null).Wait();
            Fixture = new MessageQueueFixture(Queue);
        }

        [TestMethod]
        public async Task TestSBQueueSendReceiveMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [TestMethod]
        public async Task TestSBQueueReceiveSendMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [TestMethod]
        public async Task TestSBQueueReceiveAndCompleteAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [TestMethod]
        public async Task TestSBQueueReceiveAndAbandonAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [TestMethod]
        public async Task TestSBQueueSendPeekMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [TestMethod]
        public async Task TestSBQueuePeekNoMessageAsync()
        {
            await Queue.ClearAsync(null);
            //await Fixture.TestPeekNoMessageAsync();
        }

        [TestMethod]
        public async Task TestSBQueueOnMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [TestMethod]
        // For some reasons it randomly fails
        public async Task TestSBQueueMoveToDeadMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestSBQueueMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

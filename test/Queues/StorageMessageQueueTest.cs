using Microsoft.VisualStudio.TestTools.UnitTesting;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
    [TestClass]
    public class StorageMessageQueueTest
    {
        StorageMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml");
            var connection = ConnectionParams.FromString(config.GetAsString("storage_queue"));
            Queue = new StorageMessageQueue("TestQueue", connection);
            //Queue.SetReferences(new MockReferences());
            Queue.Interval = 50;
            Queue.OpenAsync(null).Wait();

            Fixture = new MessageQueueFixture(Queue);
        }

        [TestMethod]
        public async Task TestStorageSendReceiveMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [TestMethod]
        public async Task TestStorageReceiveSendMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [TestMethod]
        public async Task TestStorageReceiveAndCompleteAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [TestMethod]
        public async Task TestStorageReceiveAndAbandonAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [TestMethod]
        public async Task TestStorageSendPeekMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [TestMethod]
        public async Task TestStoragePeekNoMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestPeekNoMessageAsync();
        }

        [TestMethod]
        public async Task TestStorageOnMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [TestMethod]
        public async Task TestStorageMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestStorageMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }

        //[TestMethod]
        //public async Task TestStorageNullMessageAsync()
        //{
        //    var envelop = await Queue.ReceiveAsync(TimeSpan.FromMilliseconds(10000000));
        //    await Queue.CompleteAsync(envelop);
        //    Assert.IsNotNull(envelop);
        //}
    }
}

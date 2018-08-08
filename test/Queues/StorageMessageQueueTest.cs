using PipServices.Commons.Config;
using PipServices.Components.Config;
using PipServices.Components.Connect;
using System.Threading.Tasks;
using Xunit;

namespace PipServices.Azure.Queues
{
    public class StorageMessageQueueTest
    {
        StorageMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        public StorageMessageQueueTest()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml", null);
            var connection = ConnectionParams.FromString(config.GetAsString("storage_queue"));
            Queue = new StorageMessageQueue("TestQueue", connection);
            //Queue.SetReferences(new MockReferences());
            Queue.Interval = 50;
            Queue.OpenAsync(null).Wait();

            Fixture = new MessageQueueFixture(Queue);
        }

        [Fact]
        public async Task TestStorageSendReceiveMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [Fact]
        public async Task TestStorageReceiveSendMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [Fact]
        public async Task TestStorageReceiveAndCompleteAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [Fact]
        public async Task TestStorageReceiveAndAbandonAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [Fact]
        public async Task TestStorageSendPeekMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [Fact]
        public async Task TestStoragePeekNoMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestPeekNoMessageAsync();
        }

        [Fact]
        public async Task TestStorageOnMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [Fact]
        public async Task TestStorageMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [Fact]
        public async Task TestStorageMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }

        //[TestMethod]
        //public async Task TestStorageNullMessageAsync()
        //{
        //    var envelope = await Queue.ReceiveAsync(TimeSpan.FromMilliseconds(10000000));
        //    await Queue.CompleteAsync(envelope);
        //    Assert.IsNotNull(envelope);
        //}
    }
}

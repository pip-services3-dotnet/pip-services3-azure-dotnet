using PipServices3.Commons.Config;
using PipServices3.Components.Config;
using PipServices3.Components.Connect;
using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Queues
{
    public class StorageMessageQueueBackwardCompatibilityTest
    {
        private StorageMessageQueue QueueOld { get; set; }
        private StorageMessageQueue QueueNew { get; set; }
        MessageQueueFixtureBackwardCompatibility Fixture { get; set; }

        public StorageMessageQueueBackwardCompatibilityTest()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml", null);
            var connection = ConnectionParams.FromString(config.GetAsString("storage_queue"));
            QueueOld = new StorageMessageQueue("TestQueue", connection) {Interval = 50};
            QueueOld.OpenAsync(null).Wait();
            
            var connection2 = ConnectionParams.FromString(config.GetAsString("storage_queue2"));
            QueueNew = new StorageMessageQueue("TestQueue", connection2) {Interval = 50};
            QueueNew.OpenAsync(null).Wait();

            Fixture = new MessageQueueFixtureBackwardCompatibility(QueueOld, QueueNew);
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageSendReceiveMessageAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestSendReceiveMessageNewAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestSendReceiveMessageNewAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageReceiveSendMessageAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageReceiveAndCompleteAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageReceiveAndAbandonAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageSendPeekMessageAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStoragePeekNoMessageAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestPeekNoMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageOnMessageAsync()
        {
            await QueueOld.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestStorageMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

using PipServices.Components.Config;
using PipServices.Components.Connect;

using System.Threading.Tasks;

using Xunit;

namespace PipServices.Azure.Queues
{
    public class ServiceBusMessageQueueTest
    {
        ServiceBusMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        public ServiceBusMessageQueueTest()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml", null);
            var connection = ConnectionParams.FromString(config.GetAsString("sb_queue"));
            Queue = new ServiceBusMessageQueue("TestQueue", connection);
            Queue.OpenAsync(null).Wait();
            Fixture = new MessageQueueFixture(Queue);
        }

        [Fact]
        public async Task TestSBQueueSendReceiveMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [Fact]
        public async Task TestSBQueueReceiveSendMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [Fact]
        public async Task TestSBQueueReceiveAndCompleteAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [Fact]
        public async Task TestSBQueueReceiveAndAbandonAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [Fact]
        public async Task TestSBQueueSendPeekMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [Fact]
        public async Task TestSBQueuePeekNoMessageAsync()
        {
            await Queue.ClearAsync(null);
            //await Fixture.TestPeekNoMessageAsync();
        }

        [Fact]
        public async Task TestSBQueueOnMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [Fact]
        // For some reasons it randomly fails
        public async Task TestSBQueueMoveToDeadMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [Fact]
        public async Task TestSBQueueMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

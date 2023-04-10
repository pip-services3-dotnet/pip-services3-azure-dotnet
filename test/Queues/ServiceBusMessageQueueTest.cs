using PipServices3.Commons.Config;
using PipServices3.Components.Config;

using System.Threading.Tasks;

using Xunit;

namespace PipServices3.Azure.Queues
{
    public class ServiceBusMessageQueueTest
    {
        ServiceBusMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        public ServiceBusMessageQueueTest()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml", null);
            var configParams = ConfigParams.FromString(config.GetAsString("sb_queue"));
            Queue = new ServiceBusMessageQueue("TestQueue", configParams);

            Queue.OpenAsync(null).Wait();

            Fixture = new MessageQueueFixture(Queue);
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestSendReceiveMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestReceiveSendMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestReceiveAndCompleteAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestReceiveAndAbandonAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestSendPeekMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestPeekNoMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestPeekNoMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestOnMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestMoveToDeadMessageAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public async Task TestMessageCountAsync()
        {
            await Queue.ClearAsync(null);
            await Fixture.TestMessageCountAsync();
        }
    }
}

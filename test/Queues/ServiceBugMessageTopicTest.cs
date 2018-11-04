using PipServices3.Components.Config;
using PipServices3.Components.Connect;

using System.Threading.Tasks;

using Xunit;

namespace PipServices3.Azure.Queues
{
    public class ServiceBusMessageTopicTest
    {
        ServiceBusMessageTopic Topic { get; set; }
        MessageQueueFixture Fixture { get; set; }

        public ServiceBusMessageTopicTest()
        {
            var config = YamlConfigReader.ReadConfig(null, @"..\\..\\..\\..\\config\\test_connections.yaml", null);
            var connection = ConnectionParams.FromString(config.GetAsString("sb_topic"));
            Topic = new ServiceBusMessageTopic("TestTopic", connection);
            Topic.OpenAsync(null).Wait();
            Fixture = new MessageQueueFixture(Topic);
        }

        [Fact]
        public async Task TestSBTopicSendReceiveMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestSendReceiveMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicReceiveSendMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestReceiveSendMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicReceiveAndCompleteAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicReceiveAndAbandonAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicSendPeekMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestSendPeekMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicPeekNoMessageAsync()
        {
            await Topic.ClearAsync(null);
            //await Fixture.TestPeekNoMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicOnMessageAsync()
        {
            await Topic.ClearAsync(null);
            await Fixture.TestOnMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [Fact]
        public async Task TestSBTopicMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

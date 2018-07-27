using PipServices.Messaging.Queues;

using System.Threading.Tasks;

using Xunit;

namespace PipServices.Azure.Queues
{
    public class MemoryMessageQueueTest
    {
        MemoryMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        public MemoryMessageQueueTest()
        {
            Queue = new MemoryMessageQueue("TestQueue");
            //Queue.SetReferences(new MockReferences());
            Queue.OpenAsync(null).Wait();

            Fixture = new MessageQueueFixture(Queue);
        }

        [Fact]
        public async Task TestMockSendReceiveMessageAsync()
        {
            await Fixture.TestSendReceiveMessageAsync();
        }

        [Fact]
        public async Task TestMockReceiveSendMessageAsync()
        {
            await Fixture.TestReceiveSendMessageAsync();
        }

        [Fact]
        public async Task TestMockReceiveAndCompleteAsync()
        {
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [Fact]
        public async Task TestMockReceiveAndAbandonAsync()
        {
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [Fact]
        public async Task TestMockSendPeekMessageAsync()
        {
            await Fixture.TestSendPeekMessageAsync();
        }

        [Fact]
        public async Task TestMockPeekNoMessageAsync()
        {
            await Fixture.TestPeekNoMessageAsync();
        }

        [Fact]
        public async Task TestMockOnMessageAsync()
        {
            await Fixture.TestOnMessageAsync();
        }

        [Fact]
        public async Task TestMockMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [Fact]
        public async Task TestMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

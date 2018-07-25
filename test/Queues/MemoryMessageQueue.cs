using Microsoft.VisualStudio.TestTools.UnitTesting;
using PipServices.Messaging.Queues;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
    [TestClass]
    public class MemoryMessageQueueTest
    {
        MemoryMessageQueue Queue { get; set; }
        MessageQueueFixture Fixture { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            Queue = new MemoryMessageQueue("TestQueue");
            //Queue.SetReferences(new MockReferences());
            Queue.OpenAsync(null).Wait();

            Fixture = new MessageQueueFixture(Queue);
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockSendReceiveMessageAsync()
        {
            await Fixture.TestSendReceiveMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockReceiveSendMessageAsync()
        {
            await Fixture.TestReceiveSendMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockReceiveAndCompleteAsync()
        {
            await Fixture.TestReceiveAndCompleteMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockReceiveAndAbandonAsync()
        {
            await Fixture.TestReceiveAndAbandonMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockSendPeekMessageAsync()
        {
            await Fixture.TestSendPeekMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockPeekNoMessageAsync()
        {
            await Fixture.TestPeekNoMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockOnMessageAsync()
        {
            await Fixture.TestOnMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMockMoveToDeadMessageAsync()
        {
            await Fixture.TestMoveToDeadMessageAsync();
        }

        [TestMethod]
        [TestCategory("Build")]
        public async Task TestMessageCountAsync()
        {
            await Fixture.TestMessageCountAsync();
        }
    }
}

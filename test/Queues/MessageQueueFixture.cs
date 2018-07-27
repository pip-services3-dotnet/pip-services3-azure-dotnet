using PipServices.Messaging.Queues;

using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace PipServices.Azure.Queues
{
    public class MessageQueueFixture
    {
        private IMessageQueue _queue;

        public MessageQueueFixture(IMessageQueue queue)
        {
            _queue = queue;
        }

        public async Task TestSendReceiveMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            await _queue.SendAsync(null, envelop1);

            var count = _queue.MessageCount;
            Assert.True(count > 0);

            var envelop2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);
        }

        public async Task TestMoveToDeadMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            await _queue.SendAsync(null, envelop1);

            var envelop2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);

            await _queue.MoveToDeadLetterAsync(envelop2);
        }

        public async Task TestReceiveSendMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");

            ThreadPool.QueueUserWorkItem(async delegate {
                Thread.Sleep(500);
                await _queue.SendAsync(null, envelop1);
            });

            var envelop2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);
        }

        public async Task TestReceiveAndCompleteMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            await _queue.SendAsync(null, envelop1);
            var envelop2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);

            await _queue.CompleteAsync(envelop2);
            //envelop2 = await _queue.PeekAsync();
            //Assert.IsNull(envelop2);
        }

        public async Task TestReceiveAndAbandonMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            await _queue.SendAsync(null, envelop1);
            var envelop2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);

            await _queue.AbandonAsync(envelop2);
            envelop2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);
        }

        public async Task TestSendPeekMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            await _queue.SendAsync(null, envelop1);
            await Task.Delay(500);
            var envelop2 = await _queue.PeekAsync(null);
            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);
        }

        public async Task TestMessageCountAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            await _queue.SendAsync(null, envelop1);
            await Task.Delay(500);
            var count = _queue.MessageCount;
            Assert.NotNull(count);
            //Assert.IsTrue(count > 0);
        }

        public async Task TestPeekNoMessageAsync()
        {
            var envelop = await _queue.PeekAsync(null);
            Assert.Null(envelop);
        }

        public async Task TestOnMessageAsync()
        {
            var envelop1 = new MessageEnvelop("123", "Test", "Test message");
            MessageEnvelop envelop2 = null;

            _queue.BeginListen(null, async (envelop, queue) => {
                envelop2 = envelop;
                await Task.Delay(0);
            });

            await _queue.SendAsync(null, envelop1);
            await Task.Delay(100);

            Assert.NotNull(envelop2);
            Assert.Equal(envelop1.MessageType, envelop2.MessageType);
            Assert.Equal(envelop1.Message, envelop2.Message);
            Assert.Equal(envelop1.CorrelationId, envelop2.CorrelationId);

            await _queue.CloseAsync(null);
        }

    }
}

using PipServices3.Messaging.Queues;

using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace PipServices3.Azure.Queues
{
    public class MessageQueueFixtureBackwardCompatibility
    {
        private readonly IMessageQueue _queueOld;
        private readonly IMessageQueue _queueNew;

        public MessageQueueFixtureBackwardCompatibility(IMessageQueue queueOld, IMessageQueue queueNew)
        {
            _queueOld = queueOld;
            _queueNew = queueNew;
        }

        public async Task TestSendReceiveMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueOld.SendAsync(null, envelope1);

            var count = await _queueOld.ReadMessageCountAsync();
            Assert.True(count > 0);

            var envelope2 = await _queueNew.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestSendReceiveMessageNewAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueNew.SendAsync(null, envelope1);

            var count = await _queueNew.ReadMessageCountAsync();
            Assert.True(count > 0);

            var envelope2 = await _queueNew.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestMoveToDeadMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueNew.SendAsync(null, envelope1);

            var envelope2 = await _queueOld.ReceiveAsync(null, 20000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queueOld.MoveToDeadLetterAsync(envelope2);
        }

        public async Task TestReceiveSendMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");

            ThreadPool.QueueUserWorkItem(async delegate {
                Thread.Sleep(500);
                await _queueOld.SendAsync(null, envelope1);
            });

            var envelope2 = await _queueNew.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestReceiveAndCompleteMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueOld.SendAsync(null, envelope1);
            var envelope2 = await _queueNew.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queueNew.CompleteAsync(envelope2);
            //envelope2 = await _queue.PeekAsync();
            //Assert.IsNull(envelope2);
        }

        public async Task TestReceiveAndAbandonMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueOld.SendAsync(null, envelope1);
            var envelope2 = await _queueNew.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queueNew.AbandonAsync(envelope2);
            envelope2 = await _queueNew.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestSendPeekMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueOld.SendAsync(null, envelope1);
            await Task.Delay(500);
            var envelope2 = await _queueNew.PeekAsync(null);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestMessageCountAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queueOld.SendAsync(null, envelope1);
            await Task.Delay(500);
            var count = await _queueNew.ReadMessageCountAsync();
            Assert.True(count > 0);
        }

        public async Task TestPeekNoMessageAsync()
        {
            var envelope = await _queueOld.PeekAsync(null);
            Assert.Null(envelope);
        }

        public async Task TestOnMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            MessageEnvelope envelope2 = null;

            _queueOld.BeginListen(null, async (envelope, queue) => {
                envelope2 = envelope;
                await Task.Delay(0);
            });

            await _queueOld.SendAsync(null, envelope1);
            await Task.Delay(500);

            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queueOld.CloseAsync(null);
        }

    }
}

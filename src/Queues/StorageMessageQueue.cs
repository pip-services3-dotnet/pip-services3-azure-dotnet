using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using PipServices3.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PipServices3.Azure.Queues
{
    public class StorageMessageQueue : MessageQueue
    {
        private long DefaultVisibilityTimeout = 60000;
        private long DefaultCheckInterval = 10000;

        private CloudQueue _queue;
        private CloudQueue _deadQueue;
        private CancellationTokenSource _cancel = new CancellationTokenSource();

        public StorageMessageQueue(string name = null)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(true, true, true, true, true, false, true, true, true);
            Interval = DefaultCheckInterval;
        }

        public StorageMessageQueue(string name, ConfigParams config)
            : this(name)
        {
            if (config != null) Configure(config);
        }

        public StorageMessageQueue(string name, CloudQueue queue)
            : this(name)
        {
            _queue = queue;
        }

        public long Interval { get; set; }

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            Interval = config.GetAsLongWithDefault("interval", Interval);
        }

        private void CheckOpened(string correlationId)
        {
            if (_queue == null)
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        public override bool IsOpen()
        {
            return _queue != null;
        }

        public async override Task OpenAsync(string correlationId, ConnectionParams connection, CredentialParams credential)
        {
            var connectionString = ConfigParams.FromTuples(
                "DefaultEndpointsProtocol", connection.Protocol ?? connection.GetAsNullableString("DefaultEndpointsProtocol") ?? "https",
                "AccountName", credential.AccessId ?? credential.GetAsNullableString("account_name") ?? credential.GetAsNullableString("AccountName"),
                "AccountKey", credential.AccessKey ?? credential.GetAsNullableString("account_key") ??credential.GetAsNullableString("AccountKey")
            ).ToString();

            _logger.Info(null, "Connecting queue {0} to {1}", Name, connectionString);

            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var client = storageAccount.CreateCloudQueueClient();

            var queueName = connection.Get("queue") ?? Name;
            _queue = client.GetQueueReference(queueName);
            await _queue.CreateIfNotExistsAsync();

            var deadName = connection.Get("dead");
            _deadQueue = deadName != null ? client.GetQueueReference(deadName) : null;

            await Task.Delay(0);
        }

        public override async Task CloseAsync(string correlationId)
        {
            _cancel.Cancel();

            _logger.Trace(correlationId, "Closed queue {0}", this);

            await Task.Delay(0);
        }

        public override long? MessageCount
        {
            get
            {
                CheckOpened(null);
                _queue.FetchAttributesAsync().Wait();
                return _queue.ApproximateMessageCount;
            }
        }

        private MessageEnvelope ToMessage(CloudQueueMessage envelope)
        {
            if (envelope == null) return null;

            MessageEnvelope message = null;

            try
            {
                message = JsonConverter.FromJson<MessageEnvelope>(envelope.AsString);
            }
            catch
            {
                // Handle broken messages gracefully
                _logger.Warn(null, "Cannot deserialize message: " + envelope.AsString);
            }

            // If message is broken or null
            if (message == null)
            {
                message = new MessageEnvelope
                {
                    Message = envelope.AsString
                };
            }

            message.SentTime = envelope.InsertionTime.HasValue
                ? envelope.InsertionTime.Value.UtcDateTime : DateTime.UtcNow;
            message.MessageId = envelope.Id;
            message.Reference = envelope;

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            var content = JsonConverter.ToJson(message);

            var envelope = new CloudQueueMessage(content);
            await _queue.AddMessageAsync(envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelope = await _queue.PeekMessageAsync();

            if (envelope == null) return null;

            var message = ToMessage(envelope);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<List<MessageEnvelope>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);
            var envelopes = await _queue.PeekMessagesAsync(messageCount);
            var messages = new List<MessageEnvelope>();

            foreach (var envelope in envelopes)
            {
                var message = ToMessage(envelope);
                if (message != null)
                    messages.Add(message);
            }

            _logger.Trace(correlationId, "Peeked {0} messages on {1}", messages.Count, this);

            return messages;
        }

        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            CloudQueueMessage envelope = null;

            do
            {
                // Read the message and exit if received
                envelope = await _queue.GetMessageAsync(TimeSpan.FromMilliseconds(DefaultVisibilityTimeout), null, null, _cancel.Token);
                if (envelope != null) break;
                if (waitTimeout <= 0) break;

                // Wait for check interval and decrement the counter
                await Task.Delay(TimeSpan.FromMilliseconds(Interval));
                waitTimeout = waitTimeout - Interval;
                if (waitTimeout <= 0) break;
            }
            while (!_cancel.Token.IsCancellationRequested);

            var message = ToMessage(envelope);

            if (message != null)
            {
                _counters.IncrementOne("queue." + Name + ".received_messages");
                _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
            }

            return message;
        }

        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);
            // Extend the message visibility
            var envelope = (CloudQueueMessage)message.Reference;
            if (envelope != null)
            {
                await _queue.UpdateMessageAsync(envelope, TimeSpan.FromMilliseconds(lockTimeout), MessageUpdateFields.Visibility);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            // Make the message immediately visible
            var envelope = (CloudQueueMessage)message.Reference;
            if (envelope != null)
            {
                await _queue.UpdateMessageAsync(envelope, TimeSpan.FromMilliseconds(0), MessageUpdateFields.Visibility);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }
        }

        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            var envelope = (CloudQueueMessage)message.Reference;
            if (envelope != null)
            {
                await _queue.DeleteMessageAsync(envelope);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            var envelope = (CloudQueueMessage)message.Reference;
            if (envelope != null)
            {
                // Resend message to dead queue if it is defined
                if (_deadQueue != null)
                {
                    await _deadQueue.CreateIfNotExistsAsync();

                    var content = JsonConverter.ToJson(message);
                    var envelope2 = new CloudQueueMessage(content);
                    await _deadQueue.AddMessageAsync(envelope2);
                }
                else
                {
                    _logger.Warn(message.CorrelationId, "No dead letter queue is defined for {0}. The message is discarded.", this);
                }

                // Remove the message from the queue
                await _queue.DeleteMessageAsync(envelope);
                message.Reference = null;

                _counters.IncrementOne("queue." + Name + ".dead_messages");
                _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
            }
        }

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelope, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Debug(correlationId, "Started listening messages at {0}", this);

            // Create new cancelation token
            _cancel = new CancellationTokenSource();

            while (!_cancel.IsCancellationRequested)
            {
                var envelope = await _queue.GetMessageAsync(TimeSpan.FromMilliseconds(DefaultVisibilityTimeout), null, null, _cancel.Token);

                if (envelope != null && !_cancel.IsCancellationRequested)
                {
                    var message = ToMessage(envelope);

                    _counters.IncrementOne("queue." + Name + ".received_messages");
                    _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);

                    try
                    {
                        await callback(message, this);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(correlationId, ex, "Failed to process the message");
                        //throw ex;
                    }
                }
                else
                {
                    // If no messages received then wait
                    await Task.Delay(TimeSpan.FromMilliseconds(Interval));
                }
            }
        }

        public override void EndListen(string correlationId)
        {
            _cancel.Cancel();
        }

        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);
            await _queue.ClearAsync();

            _logger.Trace(null, "Cleared queue {0}", this);
        }

    }
}

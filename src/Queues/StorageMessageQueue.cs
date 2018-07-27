using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using PipServices.Components.Auth;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using PipServices.Commons.Convert;
using PipServices.Commons.Errors;
using PipServices.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
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

        public override bool IsOpened()
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

        private MessageEnvelop ToMessage(CloudQueueMessage envelop)
        {
            if (envelop == null) return null;

            MessageEnvelop message = null;

            try
            {
                message = JsonConverter.FromJson<MessageEnvelop>(envelop.AsString);
            }
            catch
            {
                // Handle broken messages gracefully
                _logger.Warn(null, "Cannot deserialize message: " + envelop.AsString);
            }

            // If message is broken or null
            if (message == null)
            {
                message = new MessageEnvelop
                {
                    Message = envelop.AsString
                };
            }

            message.SentTimeUtc = envelop.InsertionTime.HasValue
                ? envelop.InsertionTime.Value.UtcDateTime : DateTime.UtcNow;
            message.MessageId = envelop.Id;
            message.Reference = envelop;

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelop message)
        {
            CheckOpened(correlationId);
            var content = JsonConverter.ToJson(message);

            var envelop = new CloudQueueMessage(content);
            await _queue.AddMessageAsync(envelop);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelop> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelop = await _queue.PeekMessageAsync();

            if (envelop == null) return null;

            var message = ToMessage(envelop);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<List<MessageEnvelop>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);
            var envelops = await _queue.PeekMessagesAsync(messageCount);
            var messages = new List<MessageEnvelop>();

            foreach (var envelop in envelops)
            {
                var message = ToMessage(envelop);
                if (message != null)
                    messages.Add(message);
            }

            _logger.Trace(correlationId, "Peeked {0} messages on {1}", messages.Count, this);

            return messages;
        }

        public override async Task<MessageEnvelop> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            CloudQueueMessage envelop = null;

            do
            {
                // Read the message and exit if received
                envelop = await _queue.GetMessageAsync(TimeSpan.FromMilliseconds(DefaultVisibilityTimeout), null, null, _cancel.Token);
                if (envelop != null) break;
                if (waitTimeout <= 0) break;

                // Wait for check interval and decrement the counter
                await Task.Delay(TimeSpan.FromMilliseconds(Interval));
                waitTimeout = waitTimeout - Interval;
                if (waitTimeout <= 0) break;
            }
            while (!_cancel.Token.IsCancellationRequested);

            var message = ToMessage(envelop);

            if (message != null)
            {
                _counters.IncrementOne("queue." + Name + ".received_messages");
                _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
            }

            return message;
        }

        public override async Task RenewLockAsync(MessageEnvelop message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);
            // Extend the message visibility
            var envelop = (CloudQueueMessage)message.Reference;
            if (envelop != null)
            {
                await _queue.UpdateMessageAsync(envelop, TimeSpan.FromMilliseconds(lockTimeout), MessageUpdateFields.Visibility);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelop message)
        {
            CheckOpened(message.CorrelationId);
            // Make the message immediately visible
            var envelop = (CloudQueueMessage)message.Reference;
            if (envelop != null)
            {
                await _queue.UpdateMessageAsync(envelop, TimeSpan.FromMilliseconds(0), MessageUpdateFields.Visibility);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }
        }

        public override async Task CompleteAsync(MessageEnvelop message)
        {
            CheckOpened(message.CorrelationId);
            var envelop = (CloudQueueMessage)message.Reference;
            if (envelop != null)
            {
                await _queue.DeleteMessageAsync(envelop);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelop message)
        {
            CheckOpened(message.CorrelationId);
            var envelop = (CloudQueueMessage)message.Reference;
            if (envelop != null)
            {
                // Resend message to dead queue if it is defined
                if (_deadQueue != null)
                {
                    await _deadQueue.CreateIfNotExistsAsync();

                    var content = JsonConverter.ToJson(message);
                    var envelop2 = new CloudQueueMessage(content);
                    await _deadQueue.AddMessageAsync(envelop2);
                }
                else
                {
                    _logger.Warn(message.CorrelationId, "No dead letter queue is defined for {0}. The message is discarded.", this);
                }

                // Remove the message from the queue
                await _queue.DeleteMessageAsync(envelop);
                message.Reference = null;

                _counters.IncrementOne("queue." + Name + ".dead_messages");
                _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
            }
        }

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelop, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Debug(correlationId, "Started listening messages at {0}", this);

            // Create new cancelation token
            _cancel = new CancellationTokenSource();

            while (!_cancel.IsCancellationRequested)
            {
                var envelop = await _queue.GetMessageAsync(TimeSpan.FromMilliseconds(DefaultVisibilityTimeout), null, null, _cancel.Token);

                if (envelop != null && !_cancel.IsCancellationRequested)
                {
                    var message = ToMessage(envelop);

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

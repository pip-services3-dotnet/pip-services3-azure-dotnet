using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using PipServices3.Messaging.Queues;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PipServices3.Azure.Queues.Version2
{
    public class StorageMessageQueueV2 : MessageQueue
    {
        private bool _backwardCompatibility = true;

        private long DefaultVisibilityTimeout = 60000;
        private long DefaultCheckInterval = 10000;

        private QueueClient _queue;
        private QueueClient _deadQueue;
        private CancellationTokenSource _cancel = new CancellationTokenSource();

        public StorageMessageQueueV2()
            : this(null)
        {
        }

        public StorageMessageQueueV2(string name)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(true, true, true, true, true, false, true, true, true);
            Interval = DefaultCheckInterval;
        }

        public StorageMessageQueueV2(string name, ConfigParams config)
            : this(name)
        {
            if (config != null)
            {
                Configure(config);

            }
        }

        public long Interval { get; set; }

        public sealed override void Configure(ConfigParams config)
        {
            base.Configure(config);
            _backwardCompatibility = config.GetAsBooleanWithDefault("backward_compatibility", true);
            Interval = config.GetAsLongWithDefault("interval", Interval);
        }

        private void CheckOpened(string correlationId)
        {
            if (_queue == null || !_queue.Exists())
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        public override bool IsOpen()
        {
            return _queue != null;
        }

        public override async Task OpenAsync(string correlationId, List<ConnectionParams> connections, CredentialParams credential)
        {
            try
            {
                var connection = connections?.FirstOrDefault();
                if (connection == null)
                {
                    throw new ArgumentNullException(nameof(connections));
                }

                var connectionString = ConfigParams.FromTuples(
                    "DefaultEndpointsProtocol", connection.Protocol ?? connection.GetAsNullableString("DefaultEndpointsProtocol") ?? "https",
                    "AccountName", credential.AccessId ?? credential.GetAsNullableString("account_name") ?? credential.GetAsNullableString("AccountName"),
                    "AccountKey", credential.AccessKey ?? credential.GetAsNullableString("account_key") ?? credential.GetAsNullableString("AccountKey")
                ).ToString();

                _logger.Info(null, "Connecting queue {0} to {1}", Name, connectionString);

                var queueName = connection.Get("queue") ?? Name;
                _queue = new QueueClient(connectionString, queueName);
                await _queue.CreateIfNotExistsAsync();

                var deadName = connection.Get("dead");
                _deadQueue = deadName != null ? new QueueClient(connectionString, deadName) : null;

            }
            catch (Exception ex)
            {
                _queue = null;
                _logger.Error(correlationId, ex, $"Failed to open queue {Name}.");
            }

            await Task.Delay(0);
        }

        public override async Task CloseAsync(string correlationId)
        {
            _cancel.Cancel();

            _logger.Trace(correlationId, "Closed queue {0}", this);

            await Task.Delay(0);
        }

        public override async Task<long> ReadMessageCountAsync()
        {
            CheckOpened(null);
            QueueProperties properties = await _queue.GetPropertiesAsync();
            return properties.ApproximateMessagesCount;
        }

        private MessageEnvelope ToMessage(QueueMessage envelope)
        {
            if (envelope == null) return null;

            MessageEnvelope message = null;
            BackwardCompatibilityMessageEnvelope oldMessage = null;

            try
            {
                message = JsonConverter.FromJson<MessageEnvelope>(envelope.Body.ToString());
                oldMessage = JsonConverter.FromJson<BackwardCompatibilityMessageEnvelope>(envelope.Body.ToString());
            }
            catch
            {
                // Handle broken messages gracefully
                _logger.Warn(null, "Cannot deserialize message: " + envelope.Body.ToString());
            }

            // If message is broken or null
            if (message == null)
            {
                message = new MessageEnvelope
                {
                    Message = envelope.Body.ToArray()
                };
            }

            message.SentTime = envelope.InsertedOn?.UtcDateTime ?? DateTime.UtcNow;
            message.Reference = envelope;

            if (oldMessage != null)
            {
                if (message.Message == null) message.SetMessageAsString(oldMessage.Message);
                message.CorrelationId = message.CorrelationId ?? oldMessage.CorrelationId;
                message.MessageType = message.MessageType ?? oldMessage.MessageType;
            }

            return message;
        }

        private MessageEnvelope ToMessage(PeekedMessage envelope)
        {
            if (envelope == null) return null;

            MessageEnvelope message = null;
            BackwardCompatibilityMessageEnvelope oldMessage = null;

            try
            {
                message = JsonConverter.FromJson<MessageEnvelope>(envelope.Body.ToString());
                oldMessage = JsonConverter.FromJson<BackwardCompatibilityMessageEnvelope>(envelope.Body.ToString());
            }
            catch
            {
                // Handle broken messages gracefully
                _logger.Warn(null, "Cannot deserialize message: " + envelope.Body.ToString());
            }

            // If message is broken or null
            if (message == null)
            {
                message = new MessageEnvelope
                {
                    Message = envelope.Body.ToArray()
                };
            }

            message.SentTime = envelope.InsertedOn?.UtcDateTime ?? DateTime.UtcNow;
            message.Reference = envelope;

            if (oldMessage != null)
            {
                if (message.Message == null) message.SetMessageAsString(oldMessage.Message);
                message.CorrelationId = message.CorrelationId ?? oldMessage.CorrelationId;
                message.MessageType = message.MessageType ?? oldMessage.MessageType;
            }

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            var envelope = FromMessage(message);
            SendReceipt sendReceipt = await _queue.SendMessageAsync(envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        private string FromMessage(MessageEnvelope message)
        {
            var oldMessage = new BackwardCompatibilityMessageEnvelope
            {
                Message = message.GetMessageAsString(),
                MessageId = message.MessageId,
                CorrelationId = message.CorrelationId,
                MessageType = message.MessageType,
                SentTime = message.SentTime
            };
            var content = _backwardCompatibility ? JsonConverter.ToJson(oldMessage) : JsonConverter.ToJson(message);

            return content;
        }

        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            PeekedMessage envelope = await _queue.PeekMessageAsync();

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
            PeekedMessage[] envelopes = await _queue.PeekMessagesAsync(messageCount);
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
            QueueMessage envelope = null;

            do
            {
                // Read the message and exit if received
                envelope = await _queue.ReceiveMessageAsync(TimeSpan.FromMilliseconds(DefaultVisibilityTimeout), _cancel.Token);
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
            var envelope = (QueueMessage)message.Reference;
            if (envelope != null)
            {
                var updateMessageResponse = await _queue.UpdateMessageAsync(envelope.MessageId, envelope.PopReceipt, visibilityTimeout: TimeSpan.FromMilliseconds(lockTimeout));
                message.Reference = envelope.Update(updateMessageResponse.Value);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            // Make the message immediately visible
            var envelope = (QueueMessage)message.Reference;
            if (envelope != null)
            {
                await _queue.UpdateMessageAsync(envelope.MessageId, envelope.PopReceipt, visibilityTimeout: TimeSpan.FromMilliseconds(0));
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }
        }

        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            var envelope = (QueueMessage)message.Reference;
            if (envelope != null)
            {
                await _queue.DeleteMessageAsync(envelope.MessageId, envelope.PopReceipt);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            var envelope = (QueueMessage)message.Reference;
            if (envelope != null)
            {
                // Resend message to dead queue if it is defined
                if (_deadQueue != null)
                {
                    await _deadQueue.CreateIfNotExistsAsync();

                    var content = JsonConverter.ToJson(message);
                    
                    await _deadQueue.SendMessageAsync(content);
                }
                else
                {
                    _logger.Warn(message.CorrelationId, "No dead letter queue is defined for {0}. The message is discarded.", this);
                }

                // Remove the message from the queue
                await _queue.DeleteMessageAsync(envelope.MessageId, envelope.PopReceipt);
                message.Reference = null;

                _counters.IncrementOne("queue." + Name + ".dead_messages");
                _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
            }
        }

        public override async Task ListenAsync(string correlationId, IMessageReceiver receiver)
        {
            CheckOpened(correlationId);
            _logger.Debug(correlationId, "Started listening messages at {0}", this);

            // Create new cancelation token
            _cancel = new CancellationTokenSource();

            while (!_cancel.IsCancellationRequested)
            {
                QueueMessage envelope = null;
                try
                {
                    envelope = await _queue.ReceiveMessageAsync(TimeSpan.FromMilliseconds(DefaultVisibilityTimeout), _cancel.Token);
                }
                catch (TaskCanceledException ex)
                {
                    _logger.Error(correlationId, ex, "Stop to receive the messages.");
                }

                if (envelope != null && !_cancel.IsCancellationRequested)
                {
                    var message = ToMessage(envelope);

                    _counters.IncrementOne("queue." + Name + ".received_messages");
                    _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);

                    try
                    {
                        await receiver.ReceiveMessageAsync(message, this);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(correlationId, ex, "Failed to process the message");
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
            await _queue.ClearMessagesAsync();

            _logger.Trace(null, "Cleared queue {0}", this);
        }

    }
}

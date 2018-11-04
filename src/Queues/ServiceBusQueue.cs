using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Errors;
using PipServices3.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace PipServices3.Azure.Queues
{
    /*
    public class ServiceBusMessageQueue : MessageQueue
    {
        private string _queueName;
        private string _connectionString;
        private QueueClient _client;
        private NamespaceManager _manager;

        public ServiceBusMessageQueue(string name = null)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(true, true, true, true, true, true, true, true, true);
        }

        public ServiceBusMessageQueue(string name, ConfigParams config)
            : this(name)
        {
            if (config != null) Configure(config);
        }

        public ServiceBusMessageQueue(string name, QueueClient client)
            : this(name)
        {
            _client = client;
        }

        private void CheckOpened(string correlationId)
        {
            if (_client == null || _manager == null)
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        public override bool IsOpen()
        {
            return _client != null && _manager != null;
        }

        public async override Task OpenAsync(string correlationId, ConnectionParams connection, CredentialParams credential)
        {
            _queueName = connection.GetAsNullableString("queue") ?? Name;

            _connectionString = ConfigParams.FromTuples(
                "Endpoint", connection.GetAsNullableString("uri") ?? connection.GetAsNullableString("Endpoint"),
                "SharedAccessKeyName", credential.AccessId ?? credential.GetAsNullableString("SharedAccessKeyName"),
                "SharedAccessKey", credential.AccessKey ?? credential.GetAsNullableString("SharedAccessKey")
            ).ToString();

            _logger.Info(null, "Connecting queue {0} to {1}", Name, _connectionString);

            _client = QueueClient.CreateFromConnectionString(_connectionString, _queueName);
            _manager = NamespaceManager.CreateFromConnectionString(_connectionString);

            await Task.Delay(0);
        }

        public override async Task CloseAsync(string correlationId)
        {
            await _client.CloseAsync();

            _logger.Trace(correlationId, "Closed queue {0}", this);
        }

        public override long? MessageCount
        {
            get
            {
                CheckOpened(null);
                var queueDescription = _manager.GetQueue(_queueName);
                return queueDescription.MessageCount;
            }
        }

        private MessageEnvelope ToMessage(BrokeredMessage envelope, bool withLock = true)
        {
            if (envelope == null) return null;

            var message = new MessageEnvelope
            {
                MessageType = envelope.ContentType,
                CorrelationId = envelope.CorrelationId,
                MessageId = envelope.MessageId,
                SentTimeUtc = envelope.EnqueuedTimeUtc
            };

            try
            {
                message.Message = envelope.GetBody<string>();
            }
            catch
            {
                var content = envelope.GetBody<Stream>();
                StreamReader reader = new StreamReader(content);
                var msg = reader.ReadToEnd();
                message.Message = msg != null ? msg : null;
            }

            if (withLock)
                message.Reference = envelope.LockToken;

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            var envelope = new BrokeredMessage(message.Message);
            envelope.ContentType = message.MessageType;
            envelope.CorrelationId = message.CorrelationId;
            envelope.MessageId = message.MessageId;

            await _client.SendAsync(envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelope = await _client.PeekAsync();
            var message = ToMessage(envelope, false);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<List<MessageEnvelope>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);
            var envelopes = await _client.PeekBatchAsync(messageCount);
            var messages = new List<MessageEnvelope>();

            foreach (var envelope in envelopes)
            {
                var message = ToMessage(envelope, false);
                if (message != null)
                    messages.Add(message);
            }

            _logger.Trace(correlationId, "Peeked {0} messages on {1}", messages.Count, this);

            return messages;
        }

        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            var envelope = await _client.ReceiveAsync(TimeSpan.FromMilliseconds(waitTimeout));
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
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await _client.RenewMessageLockAsync(reference);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await _client.AbandonAsync(reference);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }
        }

        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await _client.CompleteAsync(reference);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await _client.DeadLetterAsync(reference);
                message.Reference = null;
                _counters.IncrementOne("queue." + Name + ".dead_messages");
                _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
            }
        }

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelope, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            _client.OnMessageAsync(async envelope =>
            {
                var message = ToMessage(envelope);

                if (message != null)
                {
                    _counters.IncrementOne("queue." + Name + ".received_messages");
                    _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
                }

                try
                {
                    await callback(message, this);
                }
                catch (Exception ex)
                {
                    _logger.Error(correlationId, ex, "Failed to process the message");
                    throw ex;
                }
            });

            await Task.Delay(0);
        }

        public override void EndListen(string correlationId)
        {
            CheckOpened(correlationId);

            // Close old client
            try
            {
                if (!_client.IsClosed)
                    _client.Close();
            }
            catch
            {
                // Ignore...
            }

            // Create a new client
            _client = QueueClient.CreateFromConnectionString(_connectionString, _queueName);
        }

        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            while (true)
            {
                var envelope = await _client.ReceiveAsync(TimeSpan.FromMilliseconds(0));
                if (envelope == null) break;
                await _client.CompleteAsync(envelope.LockToken);
            }

            _logger.Trace(correlationId, "Cleared queue {0}", this);
        }
    }
    */
}

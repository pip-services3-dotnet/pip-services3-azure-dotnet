using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using PipServices.Components.Auth;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using PipServices.Commons.Errors;
using PipServices.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
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

        public override bool IsOpened()
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

        private MessageEnvelop ToMessage(BrokeredMessage envelop, bool withLock = true)
        {
            if (envelop == null) return null;

            var message = new MessageEnvelop
            {
                MessageType = envelop.ContentType,
                CorrelationId = envelop.CorrelationId,
                MessageId = envelop.MessageId,
                SentTimeUtc = envelop.EnqueuedTimeUtc
            };

            try
            {
                message.Message = envelop.GetBody<string>();
            }
            catch
            {
                var content = envelop.GetBody<Stream>();
                StreamReader reader = new StreamReader(content);
                var msg = reader.ReadToEnd();
                message.Message = msg != null ? msg : null;
            }

            if (withLock)
                message.Reference = envelop.LockToken;

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelop message)
        {
            CheckOpened(correlationId);
            var envelop = new BrokeredMessage(message.Message);
            envelop.ContentType = message.MessageType;
            envelop.CorrelationId = message.CorrelationId;
            envelop.MessageId = message.MessageId;

            await _client.SendAsync(envelop);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelop> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelop = await _client.PeekAsync();
            var message = ToMessage(envelop, false);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<List<MessageEnvelop>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);
            var envelops = await _client.PeekBatchAsync(messageCount);
            var messages = new List<MessageEnvelop>();

            foreach (var envelop in envelops)
            {
                var message = ToMessage(envelop, false);
                if (message != null)
                    messages.Add(message);
            }

            _logger.Trace(correlationId, "Peeked {0} messages on {1}", messages.Count, this);

            return messages;
        }

        public override async Task<MessageEnvelop> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            var envelop = await _client.ReceiveAsync(TimeSpan.FromMilliseconds(waitTimeout));
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
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await _client.RenewMessageLockAsync(reference);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelop message)
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

        public override async Task CompleteAsync(MessageEnvelop message)
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

        public override async Task MoveToDeadLetterAsync(MessageEnvelop message)
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

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelop, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            _client.OnMessageAsync(async envelop =>
            {
                var message = ToMessage(envelop);

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
                var envelop = await _client.ReceiveAsync(TimeSpan.FromMilliseconds(0));
                if (envelop == null) break;
                await _client.CompleteAsync(envelop.LockToken);
            }

            _logger.Trace(correlationId, "Cleared queue {0}", this);
        }
    }
}

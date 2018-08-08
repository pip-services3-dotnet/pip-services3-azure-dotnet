using PipServices.Components.Auth;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using PipServices.Commons.Errors;
using PipServices.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
    // This implementation doesn't use subscriptions. Don't use it unless you know what you are doing!
    /*
    public class ServiceBusMessageTopic2 : MessageQueue
    {
        private long DefaultCheckInterval = 10000;

        private string _topicName;
        private TopicClient _client;
        private CancellationTokenSource _cancel = new CancellationTokenSource();
        private NamespaceManager _manager;

        public ServiceBusMessageTopic2(string name = null)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(true, true, true, true, true, false, false, false, false);
            Interval = DefaultCheckInterval;
        }

        public ServiceBusMessageTopic2(string name, ConfigParams config)
            : this(name)
        {
            if (config != null) Configure(config);
        }

        public ServiceBusMessageTopic2(string name, TopicClient client)
            : this(name)
        {
            _client = client;
        }

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            Interval = config.GetAsLongWithDefault("interval", Interval);
        }

        public long Interval { get; set; }

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
            _topicName = connection.GetAsNullableString("topic") ?? Name;

            var connectionString = ConfigParams.FromTuples(
                "Endpoint", connection.GetAsNullableString("uri") ?? connection.GetAsNullableString("Endpoint"),
                "SharedAccessKeyName", credential.AccessId ?? credential.GetAsNullableString("SharedAccessKeyName"),
                "SharedAccessKey", credential.AccessKey ?? credential.GetAsNullableString("SharedAccessKey")
            ).ToString();

            _logger.Info(null, "Connecting queue {0} to {1}", Name, connectionString);

            _client = TopicClient.CreateFromConnectionString(connectionString, _topicName);
            _manager = NamespaceManager.CreateFromConnectionString(connectionString);

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
                var topicDescription = _manager.GetTopic(_topicName);
                return topicDescription.MessageCountDetails.ActiveMessageCount;
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
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);

            return message;
        }

        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            BrokeredMessage envelope = null;

            var expirationTime = DateTime.Now.AddMilliseconds(waitTimeout);
            do
            {
                // Read the message and exit if received
                envelope = await _client.PeekAsync();
                if (envelope != null) break;
                if (waitTimeout <= 0) break;

                // Wait for check interval and decrement the counter
                if (DateTime.Now >= expirationTime) break;
            }
            while (!_cancel.Token.IsCancellationRequested);

            if (envelope == null) return null;

            var message = ToMessage(envelope);

            if (message != null)
            {
                _counters.IncrementOne("queue." + Name + ".received_messages");
                _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
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

            _logger.Trace(null, "Peeked {0} messages on {1}", messages.Count, this);

            return messages;
        }

        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);
            _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);

            // Do nothing...
            await Task.Delay(0);
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            // Shall we send it back to the topic?
            await SendAsync(message.CorrelationId, message);

            _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
        }

        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);

            // Do nothing...
            await Task.Delay(0);
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            _counters.IncrementOne("queue." + Name + ".dead_messages");
            _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);

            // Do nothing...
            await Task.Delay(0);
        }

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelope, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            // Create new cancelation token
            _cancel = new CancellationTokenSource();

            while (!_cancel.IsCancellationRequested)
            {
                var envelope = await _client.PeekAsync();

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

            while (true)
            {
                var envelope = await _client.PeekAsync();
                if (envelope == null) break;
            }

            _logger.Trace(null, "Cleared queue {0}", this);
        }

    }
    */
}

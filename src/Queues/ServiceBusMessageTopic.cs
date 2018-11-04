using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Azure.ServiceBus;

namespace PipServices3.Azure.Queues
{
    /*
    public class ServiceBusMessageTopic : MessageQueue
    {
        private string _topicName;
        private string _subscriptionName;
        private string _connectionString;
        private TopicClient _topic;
        private bool _tempSubscriber;
        private SubscriptionClient _subscription;
        private NamespaceManager _manager;

        public ServiceBusMessageTopic(string name = null)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(false, true, true, true, true, true, true, true, true);
        }

        public ServiceBusMessageTopic(string name, ConfigParams config)
            : this(name)
        {
            if (config != null) Configure(config);
        }

        public ServiceBusMessageTopic(string name, TopicClient topic, SubscriptionClient subscription)
            : this(name)
        {
            _topic = topic;
            _subscription = subscription;
        }

        private void CheckOpened(string correlationId)
        {
            if (_manager == null)
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        public override bool IsOpen()
        {
            return _manager != null;
        }

        public async override Task OpenAsync(string correlationId, ConnectionParams connection, CredentialParams credential)
        {
            _topicName = connection.GetAsNullableString("topic") ?? Name;
            _tempSubscriber = connection.Get("Subscription") == null;
            _subscriptionName = connection.Get("Subscription") ?? IdGenerator.NextLong(); // "AllMessages";

            _connectionString = ConfigParams.FromTuples(
                "Endpoint", connection.GetAsNullableString("uri") ?? connection.GetAsNullableString("Endpoint"),
                "SharedAccessKeyName", credential.AccessId ?? credential.GetAsNullableString("SharedAccessKeyName"),
                "SharedAccessKey", credential.AccessKey ?? credential.GetAsNullableString("SharedAccessKey")
            ).ToString();

            _manager = NamespaceManager.CreateFromConnectionString(_connectionString);

            await Task.Delay(0);
        }

        public override async Task CloseAsync(string correlationId)
        {
            if (_topic != null && _topic.IsClosedOrClosing == false)
                await _topic.CloseAsync();

            if (_subscription != null && _subscription.IsClosedOrClosing == false)
            {
                await _subscription.CloseAsync();

                // Remove temporary subscriber
                if (_tempSubscriber == true)
                    _manager.DeleteSubscription(_topicName, _subscriptionName);
            }

            _logger.Trace(correlationId, "Closed queue {0}", this);
        }

        public override long? MessageCount
        {
            get
            {
                // Commented because for dynamic topics it may create a new subscription on every call which causes failures
                CheckOpened(null);
                var subscription = GetSubscription();
                var subscriptionDescription = _manager.GetSubscription(_topicName, _subscriptionName);
                return subscriptionDescription.MessageCount;
            }
        }

        private TopicClient GetTopic()
        {
            if (_topic == null)
            {
                lock (_lock)
                {
                    if (_topic == null)
                    {
                        _logger.Info(null, "Connecting topic {0} to Topic={1};{2}", Name, _topicName, _connectionString);

                        _topic = new TopicClient(_connectionString, _topicName);
                    }
                }
            }
            return _topic;
        }

        private SubscriptionClient GetSubscription()
        {
            if (_subscription == null)
            {
                lock (_lock)
                {
                    if (_subscription == null)
                    {
                        // Create subscript if it doesn't exist
                        if (!_manager.SubscriptionExists(_topicName, _subscriptionName))
                        {
                            if (!_tempSubscriber)
                            {
                                // Create permanent subscription
                                _manager.CreateSubscription(_topicName, _subscriptionName);
                            }
                            else
                            {
                                // Create temporary subscription
                                var description = new SubscriptionDescription(_topicName, _subscriptionName);
                                description.AutoDeleteOnIdle = TimeSpan.FromMinutes(5);
                                _manager.CreateSubscription(description);
                            }
                        }

                        _logger.Info(null, "Connecting subscription {0} to Topic={1};Subscription={2};{3}",
                            Name, _topic, _subscription, _connectionString);

                        _subscription = new SubscriptionClient(
                            _connectionString, _topicName, _subscriptionName, ReceiveMode.PeekLock);
                    }
                }
            }
            return _subscription;
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

            await GetTopic().SendAsync(envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelope = await GetSubscription().PeekAsync();
            var message = ToMessage(envelope, false);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            var envelope = await GetSubscription().ReceiveAsync(TimeSpan.FromMilliseconds(waitTimeout));
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
            var envelopes = await GetSubscription().PeekBatchAsync(messageCount);
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

        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await GetSubscription().RenewMessageLockAsync(reference);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await GetSubscription().AbandonAsync(reference);
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
                await GetSubscription().CompleteAsync(reference);
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
                await GetSubscription().DeadLetterAsync(reference);
                message.Reference = null;
                _counters.IncrementOne("queue." + Name + ".dead_messages");
            }
            _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
        }

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelope, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            GetSubscription().OnMessageAsync(async envelope =>
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
            lock (_lock)
            {
                if (_subscription != null)
                {
                    // Close open subscription
                    try
                    {
                        if (!_subscription.IsClosed)
                            _subscription.Close();
                    }
                    catch
                    {
                        // Ignore exception
                    }

                    // Remove it
                    _subscription = null;
                }
            }
        }

        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            while (true)
            {
                var envelope = await GetSubscription().ReceiveAsync(TimeSpan.FromMilliseconds(0));
                if (envelope == null) break;
                await GetSubscription().CompleteAsync(envelope.LockToken);
            }

            _logger.Trace(correlationId, "Cleared queue {0}", this);
        }
    }
    */
}

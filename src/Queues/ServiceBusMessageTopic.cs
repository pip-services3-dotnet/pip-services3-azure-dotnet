using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using PipServices.Components.Auth;
using PipServices.Commons.Config;
using PipServices.Components.Connect;
using PipServices.Commons.Data;
using PipServices.Commons.Errors;
using PipServices.Messaging.Queues;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace PipServices.Azure.Queues
{
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

        public override bool IsOpened()
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
            if (_topic != null && _topic.IsClosed == false)
                await _topic.CloseAsync();

            if (_subscription != null && _subscription.IsClosed == false)
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

                        _topic = TopicClient.CreateFromConnectionString(_connectionString, _topicName);
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

                        _subscription = SubscriptionClient.CreateFromConnectionString(
                            _connectionString, _topicName, _subscriptionName, ReceiveMode.PeekLock);
                    }
                }
            }
            return _subscription;
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

            await GetTopic().SendAsync(envelop);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelop> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelop = await GetSubscription().PeekAsync();
            var message = ToMessage(envelop, false);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<MessageEnvelop> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            var envelop = await GetSubscription().ReceiveAsync(TimeSpan.FromMilliseconds(waitTimeout));
            var message = ToMessage(envelop);

            if (message != null)
            {
                _counters.IncrementOne("queue." + Name + ".received_messages");
                _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
            }

            return message;
        }

        public override async Task<List<MessageEnvelop>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);
            var envelops = await GetSubscription().PeekBatchAsync(messageCount);
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

        public override async Task RenewLockAsync(MessageEnvelop message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);
            if (message.Reference != null)
            {
                var reference = (Guid)message.Reference;
                await GetSubscription().RenewMessageLockAsync(reference);
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelop message)
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

        public override async Task CompleteAsync(MessageEnvelop message)
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

        public override async Task MoveToDeadLetterAsync(MessageEnvelop message)
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

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelop, IMessageQueue, Task> callback)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            GetSubscription().OnMessageAsync(async envelop =>
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
                var envelop = await GetSubscription().ReceiveAsync(TimeSpan.FromMilliseconds(0));
                if (envelop == null) break;
                await GetSubscription().CompleteAsync(envelop.LockToken);
            }

            _logger.Trace(correlationId, "Cleared queue {0}", this);
        }
    }
}

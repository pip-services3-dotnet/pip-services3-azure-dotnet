using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Messaging.Queues;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

using Mossharbor.AzureWorkArounds.ServiceBus;
using IMessageReceiver = PipServices3.Messaging.Queues.IMessageReceiver;

namespace PipServices3.Azure.Queues
{
    public class ServiceBusMessageTopic : MessageQueue
    {
        private string _topicName;
        private string _subscriptionName;
        private string _connectionString;
        private bool _tempSubscriber;

        private TopicClient _topicClient;
        private SubscriptionClient _subscriptionClient;
        private NamespaceManager _namespaceManager;
        private MessageReceiver _messageReceiver;

        public ServiceBusMessageTopic(string name = null)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(false, true, true, true, true, true, true, true, true);
        }

        public ServiceBusMessageTopic(string name, ConfigParams config)
            : this(name)
        {
            if (config != null)
            {
                Configure(config);
            }
        }

        private void CheckOpened(string correlationId)
        {
            if (_namespaceManager == null || _messageReceiver == null)
            {
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
            }
        }

        public override bool IsOpen()
        {
            return _namespaceManager != null && _messageReceiver != null;
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
                
                _topicName = connection.GetAsNullableString("topic") ?? Name;
                _tempSubscriber = connection.Get("Subscription") == null;
                _subscriptionName = connection.Get("Subscription") ?? IdGenerator.NextLong(); // "AllMessages";

                _connectionString = ConfigParams.FromTuples(
                    "Endpoint", connection.GetAsNullableString("uri") ?? connection.GetAsNullableString("Endpoint"),
                    "SharedAccessKeyName", credential.AccessId ?? credential.GetAsNullableString("SharedAccessKeyName"),
                    "SharedAccessKey", credential.AccessKey ?? credential.GetAsNullableString("SharedAccessKey")
                ).ToString();

                _namespaceManager = NamespaceManager.CreateFromConnectionString(_connectionString);
                _messageReceiver = new MessageReceiver(_connectionString, EntityNameHelper.FormatSubscriptionPath(_topicName, _subscriptionName));
            }
            catch (Exception ex)
            {
                _namespaceManager = null;

                _logger.Error(correlationId, ex, $"Failed to open message topic '{Name}'.");
            }

            await Task.CompletedTask;
        }

        public override async Task CloseAsync(string correlationId)
        {
            if (_topicClient != null && _topicClient.IsClosedOrClosing == false)
            {
                await _topicClient.CloseAsync();
            }

            if (_subscriptionClient != null && _subscriptionClient.IsClosedOrClosing == false)
            {
                await _subscriptionClient.CloseAsync();

                // Remove temporary subscriber
                if (_tempSubscriber == true)
                {
                    _namespaceManager.DeleteSubscription(_topicName, _subscriptionName);
                }
            }

            _logger.Trace(correlationId, "Closed queue {0}", this);
        }

        public override async Task<long> ReadMessageCountAsync()
        {
            // Commented because for dynamic topics it may create a new subscription on every call which causes failures
            CheckOpened(null);
            var subscription = GetSubscription();
            var subscriptionDescription = _namespaceManager.GetSubscription(_topicName, _subscriptionName);
            return subscriptionDescription.MessageCount;
        }

        private TopicClient GetTopic()
        {
            if (_topicClient == null)
            {
                lock (_lock)
                {
                    if (_topicClient == null)
                    {
                        _logger.Info(null, "Connecting topic {0} to Topic={1};{2}", Name, _topicName, _connectionString);

                        _topicClient = new TopicClient(_connectionString, _topicName);
                    }
                }
            }
            return _topicClient;
        }

        private SubscriptionClient GetSubscription()
        {
            if (_subscriptionClient == null)
            {
                lock (_lock)
                {
                    if (_subscriptionClient == null)
                    {
                        // Create subscript if it doesn't exist
                        if (!_namespaceManager.SubscriptionExists(_topicName, _subscriptionName))
                        {
                            if (!_tempSubscriber)
                            {
                                // Create permanent subscription
                                _namespaceManager.CreateSubscription(_topicName, _subscriptionName);
                            }
                            else
                            {
                                // Create temporary subscription
                                var description = new SubscriptionDescription(_topicName, _subscriptionName);
                                description.AutoDeleteOnIdle = TimeSpan.FromMinutes(5);
                                _namespaceManager.CreateSubscription(description);
                            }
                        }

                        _logger.Info(null, "Connecting subscription {0} to Topic={1};Subscription={2};{3}",
                            Name, _topicClient, _subscriptionClient, _connectionString);

                        _subscriptionClient = new SubscriptionClient(
                            _connectionString, _topicName, _subscriptionName, ReceiveMode.PeekLock);
                    }
                }
            }
            return _subscriptionClient;
        }

        private MessageEnvelope ToMessage(Message envelope, bool withLock = true)
        {
            if (envelope == null) return null;

            var message = new MessageEnvelope
            {
                MessageType = envelope.ContentType,
                CorrelationId = envelope.CorrelationId,
                MessageId = envelope.MessageId,
                SentTime = envelope.ScheduledEnqueueTimeUtc
            };

            try
            {
                message.MessageBuffer = envelope.Body;
            }
            catch
            {
            }

            if (withLock)
            {
                message.Reference = envelope.SystemProperties?.LockToken;
            }

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            var envelope = new Message(message.MessageBuffer)
            {
                ContentType = message.MessageType,
                CorrelationId = message.CorrelationId,
                MessageId = message.MessageId
            };

            await GetTopic().SendAsync(envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);
        }

        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelope = await _messageReceiver.PeekAsync();
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
            var envelope = await _messageReceiver.ReceiveAsync(TimeSpan.FromMilliseconds(waitTimeout));
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

            var messages = new List<MessageEnvelope>();

            for (var count = 0; count < messageCount; count++)
            {
                messages.Add(await PeekAsync(correlationId));
            }

            _logger.Trace(correlationId, "Peeked {0} messages on {1}", messages.Count, this);

            return messages;
        }

        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference?.ToString();

            if (!string.IsNullOrWhiteSpace(reference))
            {
                await _messageReceiver.RenewLockAsync(message.Reference?.ToString());
                _logger.Trace(message.CorrelationId, "Renewed lock for message {0} at {1}", message, this);
            }
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference?.ToString();

            if (!string.IsNullOrWhiteSpace(reference))
            {
                await _messageReceiver.AbandonAsync(reference);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }
        }

        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference?.ToString();

            if (!string.IsNullOrWhiteSpace(reference))
            {
                await GetSubscription().CompleteAsync(reference);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference?.ToString();

            if (!string.IsNullOrWhiteSpace(reference))
            {
                await GetSubscription().DeadLetterAsync(reference);
                message.Reference = null;
                _counters.IncrementOne("queue." + Name + ".dead_messages");
                _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
            }
        }

        public override async Task ListenAsync(string correlationId, IMessageReceiver receiver)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            GetSubscription().RegisterMessageHandler(async (envelope, cancellationToken) =>
                {
                    var message = ToMessage(envelope);

                    if (message != null)
                    {
                        _counters.IncrementOne("queue." + Name + ".received_messages");
                        _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
                    }

                    try
                    {
                        await receiver.ReceiveMessageAsync(message, this);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(correlationId, ex, "Failed to process the message");
                        throw ex;
                    }

                    await GetSubscription().CompleteAsync(envelope.SystemProperties.LockToken);
                },
                new MessageHandlerOptions(exception => 
                    {
                        _logger.Error(correlationId, exception.Exception, "Failed to process the message");
                        return Task.CompletedTask;
                    })
                {
                    AutoComplete = false,
                    MaxConcurrentCalls = 1
                });

            await Task.CompletedTask;
        }

        public override void EndListen(string correlationId)
        {
            CheckOpened(correlationId);
            lock (_lock)
            {
                if (_subscriptionClient != null)
                {
                    // Close open subscription
                    try
                    {
                        if (_subscriptionClient != null && !_subscriptionClient.IsClosedOrClosing)
                        {
                            _subscriptionClient.CloseAsync().Wait();
                        }
                    }
                    catch
                    {
                        // Ignore exception
                    }

                    // Remove it
                    _subscriptionClient = null;
                }
            }
        }

        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            while (true)
            {
                var envelope = await _messageReceiver.ReceiveAsync(TimeSpan.FromMilliseconds(0));
                if (envelope == null)
                {
                    break;
                }

                await _messageReceiver.CompleteAsync(envelope.SystemProperties.LockToken);
            }

            _logger.Trace(correlationId, "Cleared queue {0}", this);
        }
    }
}

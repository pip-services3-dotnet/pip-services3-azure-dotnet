using Azure.Messaging.ServiceBus;

using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Connect;
using PipServices3.Commons.Data;
using PipServices3.Commons.Errors;
using PipServices3.Messaging.Queues;
using PipServices3.Commons.Convert;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using IMessageReceiver = PipServices3.Messaging.Queues.IMessageReceiver;

namespace PipServices3.Azure.Queues
{
    public class ServiceBusMessageTopic : MessageQueue
    {
        private string _topicName;
        private string _subscriptionName;
        private string _connectionString;

        private ServiceBusClient _queueClient;
        private ServiceBusSender _messageSender;
        private ServiceBusReceiver _messageReceiver;
        private ServiceBusProcessor _messageProcessor;

        private Func<ProcessMessageEventArgs, Task> _processMessageHandler;
        private Func<ProcessErrorEventArgs, Task> _processErrorHandler;

        public ServiceBusMessageTopic() 
            : this(null)
        {
        }

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
            if (_queueClient == null || _messageReceiver == null)
            {
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
            }
        }

        public override bool IsOpen()
        {
            return _queueClient != null && _messageReceiver != null;
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
                _subscriptionName = connection.GetAsNullableString("subscription") ?? connection.Get("Subscription") ?? IdGenerator.NextLong(); // "AllMessages";

                _connectionString = ConfigParams.FromTuples(
                    "Endpoint", connection.GetAsNullableString("uri") ?? connection.GetAsNullableString("Endpoint"),
                    "SharedAccessKeyName", credential.AccessId ?? credential.GetAsNullableString("shared_access_key_name") ?? credential.GetAsNullableString("SharedAccessKeyName"),
                    "SharedAccessKey", credential.AccessKey ?? credential.GetAsNullableString("shared_access_key") ?? credential.GetAsNullableString("SharedAccessKey")
                ).ToString();

                _queueClient = new ServiceBusClient(_connectionString);

                _messageSender = _queueClient.CreateSender(_topicName);
                _messageReceiver = _queueClient.CreateReceiver(_topicName, _subscriptionName);

                var options = new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = 1,
                    MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(15)
                };
                
                _messageProcessor = _queueClient.CreateProcessor(_topicName, _subscriptionName, options);
            }
            catch (Exception ex)
            {
                _queueClient = null;
                _messageSender = null;
                _messageReceiver = null;

                _logger.Error(correlationId, ex, $"Failed to open message topic '{Name}'.");
            }

            await Task.CompletedTask;
        }

        public override async Task CloseAsync(string correlationId)
        {
            await _messageSender.DisposeAsync();
            await _messageReceiver.DisposeAsync();
            await _queueClient.DisposeAsync();

            _logger.Trace(correlationId, "Closed queue {0}", this);
        }

        public override async Task<long> ReadMessageCountAsync()
        {
            CheckOpened(null);
            
            var previousSequenceNumber = -1L;
            var sequenceNumber = 0L;
            var counter = 0L;
            do
            {
                var peekMessages = await _messageReceiver.PeekMessagesAsync(int.MaxValue, sequenceNumber);

                if (peekMessages.Count > 0)
                {
                    sequenceNumber = peekMessages[^1].SequenceNumber;

                    if (sequenceNumber == previousSequenceNumber)
                    {
                        break;
                    }

                    previousSequenceNumber = sequenceNumber;
                    counter += peekMessages.Count;
                }
                else
                {
                    break;
                }
            } 
            while (true);

            return await Task.FromResult(counter);
        }

        private MessageEnvelope ToMessage(ServiceBusReceivedMessage envelope)
        {
            if (envelope == null) return null;

            var message = new MessageEnvelope
            {
                MessageType = envelope.ContentType,
                CorrelationId = envelope.CorrelationId,
                MessageId = envelope.MessageId,
                SentTime = envelope.ScheduledEnqueueTime.UtcDateTime,
                Message = envelope.Body.ToArray(),
                Reference = envelope
            };

            return message;
        }

        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            var envelope = new ServiceBusMessage(message.Message)
            {
                ContentType = message.MessageType,
                CorrelationId = message.CorrelationId,
                MessageId = message.MessageId
            };

            await _messageSender.SendMessageAsync(envelope);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Trace(message.CorrelationId, $"Sent message with message id: {envelope.MessageId} and payload {JsonConverter.ToJson(message)} via {this}");
        }

        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);
            var envelope = await _messageReceiver.PeekMessageAsync();
            var message = ToMessage(envelope);

            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return message;
        }

        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            CheckOpened(correlationId);
            var envelope = await _messageReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(waitTimeout));
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

            // Do nothing, instead MaxAutoLockRenewalDuration parameter is used in Message Receiver handler
            await Task.CompletedTask;
        }

        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference as ServiceBusReceivedMessage;

            if (reference != null)
            {
                await _messageReceiver.AbandonMessageAsync(reference);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }
        }

        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference as ServiceBusReceivedMessage;

            if (reference != null)
            {
                await _messageReceiver.CompleteMessageAsync(reference);
                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }
        }

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var reference = message.Reference as ServiceBusReceivedMessage;

            if (reference != null)
            {
                await _messageReceiver.DeadLetterMessageAsync(reference);
                message.Reference = null;
                _counters.IncrementOne("queue." + Name + ".dead_messages");
                _logger.Trace(message.CorrelationId, "Moved to dead message {0} at {1}", message, this);
            }
        }

        public override async Task ListenAsync(string correlationId, IMessageReceiver receiver)
        {
            CheckOpened(correlationId);
            _logger.Trace(correlationId, "Started listening messages at {0}", this);

            _processMessageHandler = async (arg) =>
            {
                var message = ToMessage(arg.Message);

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
                    throw;
                }

                // Don't finalize message here, it should be done by CompleteMessageAsync method
                //await _messageReceiver.CompleteMessageAsync(arg.Message);
            };

            _processErrorHandler = async (arg) =>
            {
                _logger.Error(correlationId, arg.Exception, "Failed to process the message");
                await Task.CompletedTask;
            };

            _messageProcessor.ProcessMessageAsync += _processMessageHandler;
            _messageProcessor.ProcessErrorAsync += _processErrorHandler;

            await _messageProcessor.StartProcessingAsync();

            await Task.CompletedTask;
        }

        public override void EndListen(string correlationId)
        {
            CheckOpened(correlationId);

            _messageProcessor.StopProcessingAsync().Wait();

            _messageProcessor.ProcessMessageAsync -= _processMessageHandler;
            _messageProcessor.ProcessErrorAsync -= _processErrorHandler;
        }

        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            try
            {
                while (await _messageReceiver.PeekMessageAsync() != null)
                {
                    var brokeredMessages = await _messageReceiver.ReceiveMessagesAsync(int.MaxValue);

                    var completeTasks = brokeredMessages.Select(m => Task.Run(() => _messageReceiver.CompleteMessageAsync(m))).ToArray();

                    Task.WaitAll(completeTasks);
                }

                _logger.Trace(correlationId, "Cleared queue {0}", this);
            }
            catch (Exception ex)
            {
                _logger.Error(correlationId, ex, $"Failed to clear queue {this}");
            }
        }
    }
}

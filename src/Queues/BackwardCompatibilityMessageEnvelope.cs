using System;
using System.Runtime.Serialization;
using System.Text;

namespace PipServices3.Azure.Queues;

/// <summary>
/// Allows adding additional information to old format messages. A correlation id, message id, and a message type
/// are added to the data being sent/received
/// 
/// Side note: a BackwardCompatibilityMessageEnvelope's message is stored as a buffer, so strings are converted 
/// using utf8 conversions.
/// </summary>
[DataContract]
public class BackwardCompatibilityMessageEnvelope
{
    /// <summary>
    /// Creates a new OldMessageEnvelope.
    /// </summary>
    public BackwardCompatibilityMessageEnvelope() { }

    /** The stored message. */
    [IgnoreDataMember]
    private byte[] MessageBuffer { get; set; }

    /** The unique business transaction id that is used to trace calls across components. */
    [DataMember(Name = "CorrelationId")]
    public string CorrelationId { get; set; }

    /** The message's auto-generated ID. */
    [DataMember(Name = "MessageId")]
    public string MessageId { get; set; }

    /** String value that defines the stored message's type. */
    [DataMember(Name = "MessageType")]
    public string MessageType { get; set; }

    /** The time at which the message was sent. */
    [DataMember(Name = "SentTime")]
    public DateTime SentTime { get; set; }

    /** Used for serialization */
    [DataMember(Name = "Message")]
    public string Message
    {
        get => GetMessageAsString();
        set => SetMessageAsString(value);
    }

    /// <summary>
    /// Stores the given value as a string.
    /// </summary>
    /// <param name="message">A string value for this message.</param>
    private void SetMessageAsString(string message)
    {
        MessageBuffer = message == null ? null : Encoding.UTF8.GetBytes(message);
    }

    /// <summary>
    /// Gets the value that was stored in this message as a JSON string.
    /// </summary>
    /// <typeparam name="T">the class type</typeparam>
    /// <returns>the value that was stored in this message as a JSON string.</returns>
    private string GetMessageAsString()
    {
        return this.MessageBuffer == null ? null : Encoding.UTF8.GetString(this.MessageBuffer);
    }
}
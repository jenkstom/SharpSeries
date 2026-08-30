namespace SharpSeries.DataQueues;

/// <summary>
/// Describes an IBM i data queue, as returned by <see cref="DataQueue.GetAttributesAsync"/>.
/// </summary>
public sealed class DataQueueAttributes
{
    /// <summary>The maximum entry length the queue accepts, in bytes (up to ~64K).</summary>
    public int MaxEntryLength { get; }

    /// <summary>The key length for keyed queues; zero for non-keyed queues.</summary>
    public int KeyLength { get; }

    /// <summary>Whether entries carry sender information (SENDERINF(*YES)).</summary>
    public bool SenderInformationIncluded { get; }

    /// <summary>Whether entries are forced to auxiliary storage on write.</summary>
    public bool ForcesToAuxiliaryStorage { get; }

    /// <summary>The text description of the queue.</summary>
    public string Description { get; }

    /// <summary>The queue type (FIFO, LIFO, or keyed).</summary>
    public DataQueueType QueueType { get; }

    internal DataQueueAttributes(
        int maxEntryLength,
        int keyLength,
        bool senderInformationIncluded,
        bool forcesToAuxiliaryStorage,
        string description,
        DataQueueType queueType)
    {
        MaxEntryLength = maxEntryLength;
        KeyLength = keyLength;
        SenderInformationIncluded = senderInformationIncluded;
        ForcesToAuxiliaryStorage = forcesToAuxiliaryStorage;
        Description = description;
        QueueType = queueType;
    }

    public override string ToString()
        => $"{QueueType}, max entry {MaxEntryLength} bytes, key length {KeyLength}";
}

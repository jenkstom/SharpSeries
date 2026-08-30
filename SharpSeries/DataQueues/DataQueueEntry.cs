using SharpSeries.Encoding;
using SharpSeries.HostServer;

namespace SharpSeries.DataQueues;

/// <summary>
/// A single entry read from an IBM i data queue. Entry data is an opaque byte string;
/// use <see cref="GetString"/> to decode it with a CCSID (typically the job's EBCDIC
/// CCSID, e.g. 37).
/// </summary>
public class DataQueueEntry
{
    /// <summary>The raw entry data bytes.</summary>
    public byte[] Data { get; }

    /// <summary>
    /// Information about the job that sent the entry, when the queue was created
    /// with SENDERINF(*YES); otherwise null.
    /// </summary>
    public DataQueueSenderInfo? SenderInfo { get; }

    internal DataQueueEntry(byte[] data, DataQueueSenderInfo? senderInfo)
    {
        Data = data;
        SenderInfo = senderInfo;
    }

    /// <summary>
    /// Decodes the entry data as text using the given CCSID.
    /// </summary>
    public string GetString(int ccsid) => CcsidConverter.GetString(ccsid, Data);

    /// <summary>
    /// Wraps a parsed wire-level entry, producing a <see cref="KeyedDataQueueEntry"/>
    /// when a key is present.
    /// </summary>
    internal static DataQueueEntry FromRaw(RawDataQueueEntry raw)
    {
        var senderInfo = DataQueueSenderInfo.Parse(raw.SenderInformation);
        return raw.Key != null
            ? new KeyedDataQueueEntry(raw.Key, raw.Data, senderInfo)
            : new DataQueueEntry(raw.Data, senderInfo);
    }
}

/// <summary>
/// A single entry read from a keyed IBM i data queue: the entry data plus the key
/// it was written under.
/// </summary>
public class KeyedDataQueueEntry : DataQueueEntry
{
    /// <summary>The raw key bytes the entry was written under.</summary>
    public byte[] Key { get; }

    internal KeyedDataQueueEntry(byte[] key, byte[] data, DataQueueSenderInfo? senderInfo)
        : base(data, senderInfo)
    {
        Key = key;
    }

    /// <summary>
    /// Decodes the key as text using the given CCSID.
    /// </summary>
    public string GetKeyString(int ccsid) => CcsidConverter.GetString(ccsid, Key);
}

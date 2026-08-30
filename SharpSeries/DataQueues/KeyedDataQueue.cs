using SharpSeries.Encoding;
using SharpSeries.HostServer;

namespace SharpSeries.DataQueues;

/// <summary>
/// Represents a keyed IBM i data queue. Entries are written under a key (1-256 bytes)
/// and read back by key with a comparison operator (equal, greater than, etc.).
/// Keyed queues are always consumed first-in, first-out among matching entries.
/// </summary>
public class KeyedDataQueue : DataQueue
{
    /// <summary>
    /// Initializes a keyed data queue reference.
    /// </summary>
    /// <param name="connection">An open <see cref="DataQueueConnection"/>.</param>
    /// <param name="name">The data queue name (max 10 characters).</param>
    /// <param name="library">
    /// The library containing the queue (max 10 characters), or *LIBL / *CURLIB.
    /// </param>
    public KeyedDataQueue(DataQueueConnection connection, string name, string library = "*LIBL")
        : base(connection, name, library)
    {
    }

    /// <summary>
    /// Writes an entry under a text key, encoding both with the connection's CCSID.
    /// </summary>
    public Task WriteAsync(string key, string data, CancellationToken cancellationToken = default)
        => WriteAsync(EncodeKey(key), CcsidConverter.GetBytes(Connection.Ccsid, data), cancellationToken);

    /// <summary>
    /// Writes an entry under a raw byte key.
    /// </summary>
    public Task WriteAsync(byte[] key, byte[] data, CancellationToken cancellationToken = default)
        => Connection.Session.WriteAsync(_nameBytes, _libraryBytes, key, data, cancellationToken);

    /// <summary>
    /// Reads the first entry matching the text key search, removing it from the queue.
    /// </summary>
    /// <param name="key">The key to search for, encoded with the connection's CCSID.</param>
    /// <param name="searchType">How candidate keys are compared to the search key.</param>
    /// <param name="waitSeconds">
    /// Number of seconds to wait when no entry matches: 0 = do not wait,
    /// N = wait up to N seconds, -1 = wait indefinitely.
    /// </param>
    public Task<KeyedDataQueueEntry?> ReadAsync(string key, KeySearchType searchType = KeySearchType.Equal, int waitSeconds = 0, CancellationToken cancellationToken = default)
        => ReadAsync(EncodeKey(key), searchType, waitSeconds, cancellationToken);

    /// <summary>
    /// Reads the first entry matching the raw byte key search, removing it from the queue.
    /// </summary>
    public async Task<KeyedDataQueueEntry?> ReadAsync(byte[] key, KeySearchType searchType = KeySearchType.Equal, int waitSeconds = 0, CancellationToken cancellationToken = default)
        => (KeyedDataQueueEntry?)await ReadInternal(key, searchType, waitSeconds, peek: false, cancellationToken);

    /// <summary>
    /// Reads the first entry matching the text key search without removing it.
    /// </summary>
    public Task<KeyedDataQueueEntry?> PeekAsync(string key, KeySearchType searchType = KeySearchType.Equal, int waitSeconds = 0, CancellationToken cancellationToken = default)
        => PeekAsync(EncodeKey(key), searchType, waitSeconds, cancellationToken);

    /// <summary>
    /// Reads the first entry matching the raw byte key search without removing it.
    /// </summary>
    public async Task<KeyedDataQueueEntry?> PeekAsync(byte[] key, KeySearchType searchType = KeySearchType.Equal, int waitSeconds = 0, CancellationToken cancellationToken = default)
        => (KeyedDataQueueEntry?)await ReadInternal(key, searchType, waitSeconds, peek: true, cancellationToken);

    private async Task<DataQueueEntry?> ReadInternal(byte[] key, KeySearchType searchType, int waitSeconds, bool peek, CancellationToken cancellationToken)
    {
        ValidateWait(waitSeconds);
        var raw = await Connection.Session.ReadAsync(_nameBytes, _libraryBytes, EncodeSearch(searchType), waitSeconds, peek, key, cancellationToken);
        return raw == null ? null : DataQueueEntry.FromRaw(raw);
    }

    /// <summary>
    /// Encodes a text key with the connection's CCSID, validating the 1-256 byte length.
    /// </summary>
    protected byte[] EncodeKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("Key cannot be null or empty.", nameof(key));

        byte[] keyBytes = CcsidConverter.GetBytes(Connection.Ccsid, key);
        ValidateKeyLength(keyBytes);
        return keyBytes;
    }

    /// <summary>
    /// Validates a raw key against the server's 1-256 byte limit.
    /// </summary>
    protected static void ValidateKeyLength(byte[] key)
    {
        if (key.Length < 1 || key.Length > 256)
            throw new ArgumentException($"Keys must be between 1 and 256 bytes (got {key.Length}).");
    }

    /// <summary>
    /// Encodes a search operator as the 2-byte EBCDIC operand the protocol expects
    /// (EQ, NE, LT, LE, GT, GE).
    /// </summary>
    internal static byte[] EncodeSearch(KeySearchType searchType)
        => searchType switch
        {
            KeySearchType.Equal => CcsidConverter.GetBytes(37, "EQ"),
            KeySearchType.NotEqual => CcsidConverter.GetBytes(37, "NE"),
            KeySearchType.LessThan => CcsidConverter.GetBytes(37, "LT"),
            KeySearchType.LessThanOrEqual => CcsidConverter.GetBytes(37, "LE"),
            KeySearchType.GreaterThan => CcsidConverter.GetBytes(37, "GT"),
            KeySearchType.GreaterThanOrEqual => CcsidConverter.GetBytes(37, "GE"),
            _ => throw new ArgumentOutOfRangeException(nameof(searchType)),
        };
}

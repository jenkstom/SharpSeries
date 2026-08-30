using SharpSeries.Encoding;
using SharpSeries.HostServer;

namespace SharpSeries.DataQueues;

/// <summary>
/// Represents an IBM i data queue (*DTAQ object). Supports non-keyed (FIFO/LIFO)
/// queues; for keyed queues use <see cref="KeyedDataQueue"/>.
/// Queues must already exist (e.g. created with the CRTDTAQ command); this class
/// reads, writes, and peeks entries.
/// </summary>
public class DataQueue
{
    /// <summary>The connection used for all operations on this queue.</summary>
    protected readonly DataQueueConnection Connection;

    /// <summary>The EBCDIC, space-padded 10-byte queue name used in request templates.</summary>
    protected readonly byte[] _nameBytes;

    /// <summary>The EBCDIC, space-padded 10-byte library used in request templates.</summary>
    protected readonly byte[] _libraryBytes;

    /// <summary>
    /// Initializes a data queue reference.
    /// </summary>
    /// <param name="connection">An open <see cref="DataQueueConnection"/>.</param>
    /// <param name="name">The data queue name (max 10 characters).</param>
    /// <param name="library">
    /// The library containing the queue (max 10 characters), or *LIBL / *CURLIB.
    /// </param>
    public DataQueue(DataQueueConnection connection, string name, string library = "*LIBL")
    {
        Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        Name = name;
        Library = library;
        _nameBytes = DataQueueExecutor.EncodeObjectName(name);
        _libraryBytes = DataQueueExecutor.EncodeObjectName(library);
    }

    /// <summary>The name of the data queue.</summary>
    public string Name { get; }

    /// <summary>The library containing the data queue (*LIBL or *CURLIB allowed).</summary>
    public string Library { get; }

    /// <summary>
    /// Writes an entry to the queue, encoding the text with the connection's CCSID.
    /// </summary>
    public Task WriteAsync(string data, CancellationToken cancellationToken = default)
        => WriteAsync(CcsidConverter.GetBytes(Connection.Ccsid, data), cancellationToken);

    /// <summary>
    /// Writes a raw byte entry to the queue.
    /// </summary>
    public Task WriteAsync(byte[] data, CancellationToken cancellationToken = default)
        => Connection.Session.WriteAsync(_nameBytes, _libraryBytes, key: null, entry: data, cancellationToken);

    /// <summary>
    /// Reads the oldest entry from the queue, removing it.
    /// </summary>
    /// <param name="waitSeconds">
    /// Number of seconds to wait when the queue is empty: 0 = do not wait,
    /// N = wait up to N seconds, -1 = wait indefinitely.
    /// </param>
    /// <returns>The entry, or null when no entry arrived within the wait period.</returns>
    public async Task<DataQueueEntry?> ReadAsync(int waitSeconds = 0, CancellationToken cancellationToken = default)
    {
        ValidateWait(waitSeconds);
        var raw = await Connection.Session.ReadAsync(_nameBytes, _libraryBytes, new byte[2], waitSeconds, peek: false, key: null, cancellationToken);
        return raw == null ? null : DataQueueEntry.FromRaw(raw);
    }

    /// <summary>
    /// Reads the oldest entry from the queue without removing it.
    /// </summary>
    /// <param name="waitSeconds">
    /// Number of seconds to wait when the queue is empty: 0 = do not wait,
    /// N = wait up to N seconds, -1 = wait indefinitely.
    /// </param>
    /// <returns>The entry, or null when no entry arrived within the wait period.</returns>
    public async Task<DataQueueEntry?> PeekAsync(int waitSeconds = 0, CancellationToken cancellationToken = default)
    {
        ValidateWait(waitSeconds);
        var raw = await Connection.Session.ReadAsync(_nameBytes, _libraryBytes, new byte[2], waitSeconds, peek: true, key: null, cancellationToken);
        return raw == null ? null : DataQueueEntry.FromRaw(raw);
    }

    /// <summary>
    /// Retrieves the attributes of the queue (max entry length, type, key length,
    /// sender information, description).
    /// </summary>
    public Task<DataQueueAttributes> GetAttributesAsync(CancellationToken cancellationToken = default)
        => Connection.Session.GetAttributesAsync(_nameBytes, _libraryBytes, cancellationToken);

    /// <summary>
    /// Validates the wait argument against the server's accepted range.
    /// </summary>
    protected static void ValidateWait(int waitSeconds)
    {
        if (waitSeconds < -1)
            throw new ArgumentOutOfRangeException(nameof(waitSeconds), waitSeconds, "Wait must be 0 (no wait), a positive number of seconds, or -1 to wait indefinitely.");
    }
}

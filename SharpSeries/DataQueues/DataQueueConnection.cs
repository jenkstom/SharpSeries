using SharpSeries.Data;
using SharpSeries.HostServer;
using SharpSeries.Logging;
using SharpSeries.Pool;

namespace SharpSeries.DataQueues;

/// <summary>
/// Represents a connection to the IBM i Data Queue Host Server (QZHQSSRV).
/// Uses the same connection string keys as <see cref="Db2Connection"/>
/// (Server, User ID, Password, CCSID). Physical sessions are pooled and reused
/// across connections with an identical connection string.
/// </summary>
/// <remarks>
/// A single connection owns one physical server session; requests are exchanged
/// one-at-a-time over it. A read with a wait time holds the session until an entry
/// arrives or the wait expires - use separate connections for concurrent waiters.
/// </remarks>
public sealed class DataQueueConnection : IDisposable
{
    // The builder responsible for parsing and managing the connection string properties.
    private readonly Db2ConnectionStringBuilder _connectionStringBuilder = new();

    // The underlying pooled session handling the physical network connection and
    // the data queue host server protocol.
    private DataQueueConnectionManager? _session;

    /// <summary>
    /// Initializes a new instance of the <see cref="DataQueueConnection"/> class.
    /// </summary>
    public DataQueueConnection()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataQueueConnection"/> class
    /// with the specified connection string.
    /// </summary>
    /// <param name="connectionString">
    /// e.g. "Server=myhost;User ID=MYUSER;Password=secret;CCSID=37".
    /// </param>
    public DataQueueConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Gets or sets the connection string used to open the data queue server session.
    /// </summary>
    public string ConnectionString
    {
        get => _connectionStringBuilder.ConnectionString;
        set => _connectionStringBuilder.ConnectionString = value ?? string.Empty;
    }

    /// <summary>
    /// Gets the default CCSID used to encode/decode entry data and keys
    /// (IBM i typically defaults to 37 for USA/Canada).
    /// </summary>
    public int Ccsid => _connectionStringBuilder.Ccsid;

    /// <summary>
    /// Gets whether the connection currently holds an open server session.
    /// </summary>
    public bool IsOpen => _session != null;

    /// <summary>
    /// The underlying session manager. Internal so the data queue classes can use it,
    /// but it remains hidden from end-users.
    /// </summary>
    internal DataQueueConnectionManager Session
        => _session ?? throw new InvalidOperationException("The DataQueueConnection is not open. Call OpenAsync first.");

    /// <summary>
    /// Asynchronously opens the connection: retrieves an established session from the
    /// pool or connects, authenticates, and performs the data queue exchange-attributes
    /// handshake with a new server job.
    /// </summary>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (_session != null)
            return;

        if (string.IsNullOrWhiteSpace(_connectionStringBuilder.Server))
            throw new InvalidOperationException("Connection string is missing 'Server'.");
        if (string.IsNullOrWhiteSpace(_connectionStringBuilder.UserID))
            throw new InvalidOperationException("Connection string is missing 'User ID'.");

        Db2Logger.Trace($"[{nameof(DataQueueConnection)}] Opening data queue connection to {_connectionStringBuilder.Server}");

        _session = await HostServerConnectionPool<DataQueueConnectionManager>.GetConnectionAsync(
            "dtaq|" + _connectionStringBuilder.ConnectionString,
            async ct =>
            {
                var session = new DataQueueConnectionManager();
                await session.ConnectAndAuthenticateAsync(
                    _connectionStringBuilder.Server,
                    _connectionStringBuilder.UserID,
                    _connectionStringBuilder.Password,
                    ct);
                return session;
            },
            cancellationToken);
    }

    /// <summary>
    /// Closes the connection. Instead of dropping the physical session, this returns it
    /// to the pool to be reused by future connections with the same connection string.
    /// </summary>
    public void Close()
    {
        if (_session != null)
        {
            HostServerConnectionPool<DataQueueConnectionManager>.ReturnConnection(
                "dtaq|" + _connectionStringBuilder.ConnectionString,
                _session);
            _session = null;
        }
    }

    /// <summary>
    /// Closes the connection (see <see cref="Close"/>).
    /// </summary>
    public void Dispose() => Close();
}

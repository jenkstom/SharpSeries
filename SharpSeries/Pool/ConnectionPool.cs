using SharpSeries.HostServer;
using SharpSeries.Logging;

namespace SharpSeries.Pool;

/// <summary>
/// A thread-safe, static connection pooling manager for SQL database sessions.
/// Because establishing a new physical TCP connection and completing the DRDA cryptographic
/// handshake with an IBM i Host Server is expensive mathematically and chronologically,
/// this pool retains physical connections even after the ADO.NET <see cref="Data.Db2Connection"/> is closed.
/// Future database requests using the identical connection string will reuse an idle connection from this pool.
/// Delegates to the generic <see cref="HostServerConnectionPool{TSession}"/>.
/// </summary>
public static class ConnectionPool
{
    /// <summary>
    /// Retrieves a pooled physical database connection or establishes a new one if the pool queue is empty.
    /// </summary>
    /// <param name="connectionString">The full connection string. Used as the unique pooling cache key.</param>
    /// <param name="host">The Host name or IP address of the IBM i system.</param>
    /// <param name="port">The physical port (ignored for now as the mapper handles it).</param>
    /// <param name="user">The user profile.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token to abort connection attempts if taking too long.</param>
    /// <returns>A connected and authenticated <see cref="HostServerConnectionManager"/> ready for DRDA commands.</returns>
    public static async Task<HostServerConnectionManager> GetConnectionAsync(
        string connectionString, string host, int port, string user, string password, CancellationToken cancellationToken)
    {
        Db2Logger.Trace($"[{nameof(ConnectionPool)}] Requesting connection to {host}:{port}");

        return await HostServerConnectionPool<HostServerConnectionManager>.GetConnectionAsync(
            connectionString,
            async ct =>
            {
                var newConnection = new HostServerConnectionManager();
                await newConnection.ConnectAndAuthenticateAsync(host, port, user, password, ct);
                return newConnection;
            },
            cancellationToken);
    }

    /// <summary>
    /// Relinquishes a physical connection back to the thread-safe connection pool queue for later reuse.
    /// This happens automatically when an ADO.NET Db2Connection is Closed or Disposed.
    /// </summary>
    public static void ReturnConnection(string connectionString, HostServerConnectionManager connection)
        => HostServerConnectionPool<HostServerConnectionManager>.ReturnConnection(connectionString, connection);

    /// <summary>
    /// Forcefully empties all pooled connection queues and severs their underlying TCP/IP sockets.
    /// Useful during application shutdown or when flushing stale/broken connection states.
    /// </summary>
    public static void ClearAllPools()
        => HostServerConnectionPool<HostServerConnectionManager>.ClearAllPools();
}

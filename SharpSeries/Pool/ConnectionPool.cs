using System.Collections.Concurrent;
using SharpSeries.HostServer;
using SharpSeries.Logging;

namespace SharpSeries.Pool;

/// <summary>
/// A thread-safe, static connection pooling manager.
/// Because establishing a new physical TCP connection and completing the DRDA cryptographic
/// handshake with an IBM i Host Server is expensive mathematically and chronologically,
/// this pool retains physical connections even after the ADO.NET <see cref="Data.Db2Connection"/> is closed.
/// Future database requests using the identical connection string will reuse an idle connection from this pool.
/// </summary>
public static class ConnectionPool
{
    // A thread-safe dictionary mapping unique Connection Strings to Queues of active Host Server connections.
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<HostServerConnectionManager>> _pools = new();

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

        // Acquire the specific queue for this exact connection string
        var pool = _pools.GetOrAdd(connectionString, _ => new ConcurrentQueue<HostServerConnectionManager>());
        
        // Attempt to dequeue a waiting connection
        if (pool.TryDequeue(out var connection))
        {
            Db2Logger.Debug($"[{nameof(ConnectionPool)}] Connection retrieved from pool. Remaining in pool: {pool.Count}");
            
            // NOTE: In a mature production scenario, we would verify socket health before returning this.
            // i.e., issue a lightweight DRDA EXCSAT (Exchange Server Attributes) ping to ensure the server hasn't dropped the link.
            return connection;
        }

        Db2Logger.Info($"[{nameof(ConnectionPool)}] Pool empty. Creating new underlying physical connection to {host}:{port}");
        
        // Construct and authenticate a brand new physical connection over the network
        var newConnection = new HostServerConnectionManager();
        await newConnection.ConnectAndAuthenticateAsync(host, port, user, password, cancellationToken);
        return newConnection;
    }

    /// <summary>
    /// Relinquishes a physical connection back to the thread-safe connection pool queue for later reuse.
    /// This happens automatically when an ADO.NET Db2Connection is Closed or Disposed.
    /// </summary>
    /// <param name="connectionString">The connection string key indexing the pool.</param>
    /// <param name="connection">The active physical connection to store.</param>
    public static void ReturnConnection(string connectionString, HostServerConnectionManager connection)
    {
        var pool = _pools.GetOrAdd(connectionString, _ => new ConcurrentQueue<HostServerConnectionManager>());
        pool.Enqueue(connection);
        Db2Logger.Debug($"[{nameof(ConnectionPool)}] Connection returned to pool. Pool size is now: {pool.Count}");
    }
    
    /// <summary>
    /// Forcefully empties all pooled connection queues and severs their underlying TCP/IP sockets.
    /// Useful during application shutdown or when flushing stale/broken connection states.
    /// </summary>
    public static void ClearAllPools()
    {
        Db2Logger.Info($"[{nameof(ConnectionPool)}] Clearing all pools");
        foreach (var pool in _pools.Values)
        {
            // Drain the queue until empty
            while (pool.TryDequeue(out var conn))
            {
                // Disconnect the physical socket forcefully
                conn.Disconnect();
            }
        }
    }
}

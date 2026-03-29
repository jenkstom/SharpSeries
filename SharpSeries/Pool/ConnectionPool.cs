using System.Collections.Concurrent;
using SharpSeries.HostServer;
using SharpSeries.Logging;

namespace SharpSeries.Pool;

public static class ConnectionPool
{
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<HostServerConnectionManager>> _pools = new();

    /// <summary>
    /// Retrieves a pooled connection or creates a new one if the pool is empty.
    /// </summary>
    public static async Task<HostServerConnectionManager> GetConnectionAsync(
        string connectionString, string host, int port, string user, string password, CancellationToken cancellationToken)
    {
        Db2Logger.Trace($"[{nameof(ConnectionPool)}] Requesting connection to {host}:{port}");

        var pool = _pools.GetOrAdd(connectionString, _ => new ConcurrentQueue<HostServerConnectionManager>());
        
        if (pool.TryDequeue(out var connection))
        {
            Db2Logger.Debug($"[{nameof(ConnectionPool)}] Connection retrieved from pool. Remaining in pool: {pool.Count}");
            // In a real scenario, we might issue an EXCSAT or lightweight ping to ensure it's still healthy
            return connection;
        }

        Db2Logger.Info($"[{nameof(ConnectionPool)}] Pool empty. Creating new underlying physical connection to {host}:{port}");
        // Pool empty, create and authenticate new underlying physics connection
        var newConnection = new HostServerConnectionManager();
        await newConnection.ConnectAndAuthenticateAsync(host, port, user, password, cancellationToken);
        return newConnection;
    }

    /// <summary>
    /// Returns a connection to the thread-safe connection pool.
    /// </summary>
    public static void ReturnConnection(string connectionString, HostServerConnectionManager connection)
    {
        var pool = _pools.GetOrAdd(connectionString, _ => new ConcurrentQueue<HostServerConnectionManager>());
        pool.Enqueue(connection);
        Db2Logger.Debug($"[{nameof(ConnectionPool)}] Connection returned to pool. Pool size is now: {pool.Count}");
    }
    
    /// <summary>
    /// Forcefully clears all pooled connections and frees raw sockets.
    /// </summary>
    public static void ClearAllPools()
    {
        Db2Logger.Info($"[{nameof(ConnectionPool)}] Clearing all pools");
        foreach (var pool in _pools.Values)
        {
            while (pool.TryDequeue(out var conn))
            {
                conn.Disconnect();
            }
        }
    }
}

using System.Collections.Concurrent;
using SharpSeries.HostServer;
using SharpSeries.Logging;

namespace SharpSeries.Pool;

/// <summary>
/// A thread-safe, static connection pooling manager generic over the host-server
/// session type (SQL database, data queue, ...).
/// Because establishing a new physical TCP connection and completing the host-server
/// cryptographic handshake is expensive, this pool retains physical sessions even
/// after the owning connection object is closed.
/// Future requests using the identical pool key will reuse an idle session.
/// </summary>
public static class HostServerConnectionPool<TSession> where TSession : HostServerSessionBase
{
    // A thread-safe dictionary mapping unique pool keys to queues of live host-server sessions.
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<TSession>> _pools = new();

    /// <summary>
    /// Retrieves a pooled session or creates and authenticates a new one via
    /// <paramref name="factory"/> if the pool queue is empty.
    /// </summary>
    /// <param name="poolKey">The unique pooling cache key (typically the connection string).</param>
    /// <param name="factory">Creates and connects a brand new session on demand.</param>
    /// <param name="cancellationToken">A token to abort connection attempts if taking too long.</param>
    public static async Task<TSession> GetConnectionAsync(string poolKey, Func<CancellationToken, Task<TSession>> factory, CancellationToken cancellationToken)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionPool<TSession>)}] Requesting session for pool key {poolKey}");

        // Acquire the specific queue for this exact key
        var pool = _pools.GetOrAdd(poolKey, _ => new ConcurrentQueue<TSession>());

        // Attempt to dequeue a waiting session
        if (pool.TryDequeue(out var session))
        {
            Db2Logger.Debug($"[{nameof(HostServerConnectionPool<TSession>)}] Session retrieved from pool. Remaining in pool: {pool.Count}");

            // NOTE: In a mature production scenario, we would verify socket health before returning this.
            // i.e. issue a lightweight request to ensure the server hasn't dropped the link.
            return session;
        }

        Db2Logger.Info($"[{nameof(HostServerConnectionPool<TSession>)}] Pool empty. Creating new underlying physical session.");

        // Construct and authenticate a brand new physical session over the network
        return await factory(cancellationToken);
    }

    /// <summary>
    /// Relinquishes a physical session back to the thread-safe pool queue for later reuse.
    /// </summary>
    public static void ReturnConnection(string poolKey, TSession session)
    {
        var pool = _pools.GetOrAdd(poolKey, _ => new ConcurrentQueue<TSession>());
        pool.Enqueue(session);
        Db2Logger.Debug($"[{nameof(HostServerConnectionPool<TSession>)}] Session returned to pool. Pool size is now: {pool.Count}");
    }

    /// <summary>
    /// Forcefully empties all pooled session queues and severs their underlying TCP/IP sockets.
    /// Useful during application shutdown or when flushing stale/broken session states.
    /// </summary>
    public static void ClearAllPools()
    {
        Db2Logger.Info($"[{nameof(HostServerConnectionPool<TSession>)}] Clearing all pools");
        foreach (var pool in _pools.Values)
        {
            // Drain the queue until empty
            while (pool.TryDequeue(out var session))
            {
                // Disconnect the physical socket forcefully
                session.Disconnect();
            }
        }
    }
}

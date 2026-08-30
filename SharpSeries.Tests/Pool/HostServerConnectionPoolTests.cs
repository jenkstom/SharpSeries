using SharpSeries.HostServer;
using SharpSeries.Pool;
using Xunit;

namespace SharpSeries.Tests.Pool;

/// <summary>
/// Tests the generic host-server session pool without touching the network:
/// sessions are handed out by a counting factory.
/// </summary>
public class HostServerConnectionPoolTests
{
    [Fact]
    public async Task ReturnedSessionIsReusedWithoutInvokingFactory()
    {
        HostServerConnectionPool<HostServerConnectionManager>.ClearAllPools();

        int factoryCalls = 0;
        Task<HostServerConnectionManager> Factory(CancellationToken ct)
        {
            factoryCalls++;
            return Task.FromResult(new HostServerConnectionManager());
        }

        var first = await HostServerConnectionPool<HostServerConnectionManager>.GetConnectionAsync("test-key", Factory, CancellationToken.None);
        HostServerConnectionPool<HostServerConnectionManager>.ReturnConnection("test-key", first);

        var second = await HostServerConnectionPool<HostServerConnectionManager>.GetConnectionAsync("test-key", Factory, CancellationToken.None);

        Assert.Same(first, second);
        Assert.Equal(1, factoryCalls);

        HostServerConnectionPool<HostServerConnectionManager>.ClearAllPools();
    }

    [Fact]
    public async Task DistinctKeysGetDistinctSessions()
    {
        HostServerConnectionPool<DataQueueConnectionManager>.ClearAllPools();

        int factoryCalls = 0;
        Task<DataQueueConnectionManager> Factory(CancellationToken ct)
        {
            factoryCalls++;
            return Task.FromResult(new DataQueueConnectionManager());
        }

        var a = await HostServerConnectionPool<DataQueueConnectionManager>.GetConnectionAsync("key-a", Factory, CancellationToken.None);
        var b = await HostServerConnectionPool<DataQueueConnectionManager>.GetConnectionAsync("key-b", Factory, CancellationToken.None);

        Assert.NotSame(a, b);
        Assert.Equal(2, factoryCalls);

        HostServerConnectionPool<DataQueueConnectionManager>.ClearAllPools();
    }
}

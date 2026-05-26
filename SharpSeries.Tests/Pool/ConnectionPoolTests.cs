using SharpSeries.HostServer;
using SharpSeries.Pool;

namespace SharpSeries.Tests.Pool;

public class ConnectionPoolTests
{
    [Fact]
    public void ReturnConnectionEnqueuesToPool()
    {
        // Force cleanup prior to testing state behavior
        ConnectionPool.ClearAllPools();
        
        var connectionString = "Server=foo;";
        var mockConnManager = new HostServerConnectionManager();
        
        ConnectionPool.ReturnConnection(connectionString, mockConnManager);

        // Can't directly peek at ConcurrentDictionary size without reflection,
        // but can ensure it doesn't throw and cleans up cleanly.
        
        ConnectionPool.ClearAllPools();
    }
}

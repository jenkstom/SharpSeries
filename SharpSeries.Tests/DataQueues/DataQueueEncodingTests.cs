using SharpSeries.DataQueues;
using SharpSeries.HostServer;
using Xunit;
using static SharpSeries.Tests.HostServer.HostServerSessionBaseTests;

namespace SharpSeries.Tests.DataQueues;

/// <summary>
/// Tests object-name/key/search encoding and argument validation for the public
/// data queue API.
/// </summary>
public class DataQueueEncodingTests
{
    [Fact]
    public void EncodeObjectNameUpperCasesAndPadsToTenBytes()
    {
        byte[] encoded = DataQueueExecutor.EncodeObjectName("qtest");
        Assert.Equal(10, encoded.Length);
        Assert.Equal(Ebcdic("QTEST     "), encoded);
    }

    [Theory]
    [InlineData("*LIBL")]
    [InlineData("*CURLIB")]
    [InlineData("MYLIB10CHR")] // exactly 10 characters
    public void EncodeObjectNameAcceptsValidValues(string name)
    {
        Assert.Equal(10, DataQueueExecutor.EncodeObjectName(name).Length);
    }

    [Theory]
    [InlineData("TOOLONGNAME11")]
    [InlineData("")]
    [InlineData("   ")]
    public void EncodeObjectNameRejectsInvalidValues(string name)
    {
        Assert.Throws<ArgumentException>(() => DataQueueExecutor.EncodeObjectName(name));
    }

    [Theory]
    [InlineData(KeySearchType.Equal, "EQ")]
    [InlineData(KeySearchType.NotEqual, "NE")]
    [InlineData(KeySearchType.LessThan, "LT")]
    [InlineData(KeySearchType.LessThanOrEqual, "LE")]
    [InlineData(KeySearchType.GreaterThan, "GT")]
    [InlineData(KeySearchType.GreaterThanOrEqual, "GE")]
    public void EncodeSearchProducesTwoByteEbcdicOperand(KeySearchType searchType, string operand)
    {
        byte[] encoded = KeyedDataQueue.EncodeSearch(searchType);
        Assert.Equal(2, encoded.Length);
        Assert.Equal(Ebcdic(operand), encoded);
    }

    [Fact]
    public void WriteRejectsSearchOperandThatIsNotTwoBytes()
    {
        byte[] buffer = new byte[300];
        Assert.Throws<ArgumentException>(
            () => DataQueueExecutor.WriteRead(buffer, DataQueueExecutor.EncodeObjectName("Q"),
                DataQueueExecutor.EncodeObjectName("*LIBL"), new byte[3], 0, false, null, out _));
    }

    [Fact]
    public async Task ReadRejectsWaitBelowMinusOne()
    {
        var queue = new DataQueue(new DataQueueConnection(), "QTEST");
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => queue.ReadAsync(-5));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => queue.PeekAsync(-2));
    }

    [Fact]
    public async Task WriteRejectsKeysOutsideOneTo256Bytes()
    {
        var queue = new KeyedDataQueue(new DataQueueConnection(), "QTEST");

        // Key validation happens before any network activity, so no open connection is needed.
        await Assert.ThrowsAsync<ArgumentException>(() => queue.WriteAsync("", "data"));
        await Assert.ThrowsAsync<ArgumentException>(() => queue.WriteAsync(new string('K', 257), "data"));
    }

    [Fact]
    public void ConnectionStringExposesCcsid()
    {
        var connection = new DataQueueConnection("Server=myhost;User ID=ME;Password=secret;CCSID=273");
        Assert.Equal(273, connection.Ccsid);
        // DbConnectionStringBuilder returns the canonicalized string, not the original text.
        Assert.StartsWith("server=myhost", connection.ConnectionString);
    }

    [Fact]
    public void SessionThrowsWhenConnectionNotOpen()
    {
        var connection = new DataQueueConnection("Server=myhost");
        Assert.False(connection.IsOpen);
        Assert.Throws<InvalidOperationException>(() => connection.Session);
    }
}

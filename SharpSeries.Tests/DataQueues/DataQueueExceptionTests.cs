using SharpSeries.DataQueues;
using Xunit;
using static SharpSeries.Tests.HostServer.HostServerSessionBaseTests;

namespace SharpSeries.Tests.DataQueues;

/// <summary>
/// Tests the return-code/message-id mapping in <see cref="DataQueueException.Create"/>,
/// ported from JTOpen BaseDataQueueImplRemote.buildException().
/// </summary>
public class DataQueueExceptionTests
{
    [Fact]
    public void CommandCheckWithQueueNotFoundMapsToObjectId()
    {
        var ex = DataQueueException.Create(0xF001, Ebcdic("CPF9801 Object QTEST in library MYLIB not found."), "write");

        Assert.Equal(0xF001, ex.ReturnCode);
        Assert.Equal("CPF9801", ex.MessageId);
        Assert.Contains("does not exist", ex.Message);
    }

    [Fact]
    public void CommandCheckWithLibraryNotFoundMapsToLibraryId()
    {
        var ex = DataQueueException.Create(0xF001, Ebcdic("CPF9810 Library MYLIB not found."), "write");

        Assert.Equal("CPF9810", ex.MessageId);
        Assert.Contains("library", ex.Message);
    }

    [Fact]
    public void CommandCheckWithKeyedMismatchMapsToTypeMismatch()
    {
        var ex = DataQueueException.Create(0xF001, Ebcdic("CPF9502 Queue is not keyed."), "write");

        Assert.Equal("CPF9502", ex.MessageId);
        Assert.Contains("keyed", ex.Message);
    }

    [Theory]
    [InlineData(0xF002, "protocol")]
    [InlineData(0xF003, "syntax")]
    [InlineData(0xF004, "destroyed")]
    [InlineData(0xF005, "length")]
    [InlineData(0xF009, "exit program")]
    public void KnownReturnCodesMapToDescriptions(int rc, string expectedFragment)
    {
        var ex = DataQueueException.Create(rc, null, "write");

        Assert.Equal(rc, ex.ReturnCode);
        Assert.Null(ex.MessageId);
        Assert.Contains(expectedFragment, ex.Message.ToLowerInvariant());
    }

    [Fact]
    public void UnknownReturnCodeFallsBackToGenericMessage()
    {
        var ex = DataQueueException.Create(0xF123, Ebcdic("Something odd happened."), "read");

        Assert.Equal(0xF123, ex.ReturnCode);
        Assert.Contains("Something odd happened.", ex.Message);
    }

    [Fact]
    public void UnknownReturnCodeWithoutMessageStillReports()
    {
        var ex = DataQueueException.Create(0xF123, null, "peek");

        Assert.Contains("0xF123", ex.Message);
        Assert.Contains("peek", ex.Message);
    }
}

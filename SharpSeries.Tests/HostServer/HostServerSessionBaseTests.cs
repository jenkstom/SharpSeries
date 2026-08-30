using System.Buffers.Binary;
using SharpSeries.Encoding;
using SharpSeries.HostServer;
using Xunit;

namespace SharpSeries.Tests.HostServer;

/// <summary>
/// Byte-level golden tests for the shared host-server session plumbing:
/// the Server Mapper request and the standard 20-byte packet header.
/// Expected bytes derived from JTOpen AS400PortMapDS.java and ClientAccessDataStream.java.
/// </summary>
public class HostServerSessionBaseTests
{
    [Fact]
    public void MapperRequestIsPlainAsciiServiceName()
    {
        // JTOpen AS400PortMapDS: the mapper request is just the ASCII service name,
        // and the reply is '+' (0x2B) followed by the 4-byte big-endian port.
        Assert.Equal(
            new byte[] { 0x61, 0x73, 0x2D, 0x64, 0x74, 0x61, 0x71 }, // "as-dtaq"
            HostServerSessionBase.BuildServiceMapperRequest("as-dtaq"));

        Assert.Equal(
            new byte[] { 0x61, 0x73, 0x2D, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65 }, // "as-database"
            HostServerSessionBase.BuildServiceMapperRequest("as-database"));
    }

    [Fact]
    public void EnvelopeWritesHeaderFieldsAtProtocolOffsets()
    {
        byte[] buffer = new byte[20];
        HostServerSessionBase.WriteHostServerEnvelope(buffer, serverId: 0xE004, length: 40, templateLength: 20, reqRepId: 0x1807, correlation: 3);

        // Total length (bytes 0-3)
        Assert.Equal(40, BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4)));
        // Header ID / flags (bytes 4-5)
        Assert.Equal(0, buffer[4]);
        Assert.Equal(0, buffer[5]);
        // Server ID (bytes 6-7)
        Assert.Equal(0xE0, buffer[6]);
        Assert.Equal(0x04, buffer[7]);
        // CS instance (bytes 8-11)
        Assert.Equal(0, BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(8, 4)));
        // Correlation (bytes 12-15)
        Assert.Equal(3, BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(12, 4)));
        // Template length (bytes 16-17)
        Assert.Equal(20, BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(16, 2)));
        // ReqRep ID (bytes 18-19)
        Assert.Equal(0x1807, BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(18, 2)));
    }

    [Fact]
    public void DatabaseSessionUsesAsDatabaseIdentity()
    {
        // Regression guard for the Phase 0 refactor: the SQL session must keep
        // the as-database identity (server ID, service name, fallback port).
        var db = new HostServerConnectionManager();
        Assert.Equal(0xE004, HostServerConnectionManager.DatabaseServerId);
        Assert.Equal(0xE007, DataQueueExecutor.DataQueueServerId);
        Assert.NotEqual(HostServerConnectionManager.DatabaseServerId, DataQueueExecutor.DataQueueServerId);
    }

    /// <summary>Concatenates byte fragments into a single expected packet.</summary>
    internal static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (var part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }
        return result;
    }

    /// <summary>EBCDIC (CCSID 37) bytes of a string.</summary>
    internal static byte[] Ebcdic(string s) => CcsidConverter.GetBytes(37, s);
}

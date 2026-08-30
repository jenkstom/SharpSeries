using System.Buffers.Binary;
using SharpSeries.DataQueues;
using SharpSeries.HostServer;
using Xunit;
using static SharpSeries.Tests.HostServer.HostServerSessionBaseTests;

namespace SharpSeries.Tests.HostServer;

/// <summary>
/// Tests for the data queue reply parsers, using synthetic packets assembled with the
/// layouts documented in JTOpen's DQReadNormalReplyDataStream, DQCommonReplyDataStream,
/// and DQRequestAttributesNormalReplyDataStream.
/// </summary>
public class DataQueueReplyParserTests
{
    /// <summary>Builds a reply packet: 20-byte header + body, with correct total length.</summary>
    private static byte[] Reply(ushort replyId, params byte[][] bodyParts)
    {
        int bodyLength = bodyParts.Sum(p => p.Length);
        byte[] packet = new byte[20 + bodyLength];
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0, 4), (uint)packet.Length);
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(18, 2), replyId);
        int offset = 20;
        foreach (var part in bodyParts)
        {
            part.CopyTo(packet, offset);
            offset += part.Length;
        }
        return packet;
    }

    private static byte[] Llcp(ushort codePoint, byte[] data)
    {
        byte[] block = new byte[6 + data.Length];
        BinaryPrimitives.WriteUInt32BigEndian(block.AsSpan(0, 4), (uint)(6 + data.Length));
        BinaryPrimitives.WriteUInt16BigEndian(block.AsSpan(4, 2), codePoint);
        data.CopyTo(block, 6);
        return block;
    }

    private static byte[] SenderBlock(string job, string user, string number, string profile)
        => Ebcdic(job.PadRight(10) + user.PadRight(10) + number.PadRight(6) + profile.PadRight(10));

    [Fact]
    public void ParseReadReplyExtractsEntryKeyAndSenderInfo()
    {
        byte[] reply = Reply(
            DataQueueExecutor.ReadReplyId,
            new byte[2],                                  // bytes 20-21 (unused by parser)
            SenderBlock("MYJOB", "MYUSER", "123456", "ME"),
            Llcp(0x5001, Ebcdic("TEST")),
            Llcp(0x5002, Ebcdic("K1")),
            Llcp(0x5005, new byte[] { 0xFF }));           // unknown parameter: must be skipped

        var raw = DataQueueExecutor.ParseReadReply(reply);

        Assert.Equal(Ebcdic("TEST"), raw.Data);
        Assert.Equal(Ebcdic("K1"), raw.Key);

        Assert.NotNull(raw.SenderInformation);
        var entry = DataQueueEntry.FromRaw(raw);
        Assert.IsType<KeyedDataQueueEntry>(entry);
        Assert.Equal("TEST", entry.GetString(37));
        Assert.Equal("K1", ((KeyedDataQueueEntry)entry).GetKeyString(37));

        Assert.NotNull(entry.SenderInfo);
        Assert.Equal("MYJOB", entry.SenderInfo.JobName);
        Assert.Equal("MYUSER", entry.SenderInfo.UserName);
        Assert.Equal("123456", entry.SenderInfo.JobNumber);
        Assert.Equal("ME", entry.SenderInfo.CurrentUserProfile);
    }

    [Fact]
    public void ParseReadReplyWithoutSenderInfoYieldsNullSender()
    {
        byte[] emptySender = new byte[36]; // starts with 0x00, but parser checks EBCDIC space...
        // The server pads unused sender info with EBCDIC spaces (0x40).
        Array.Fill(emptySender, (byte)0x40);

        byte[] reply = Reply(
            DataQueueExecutor.ReadReplyId,
            new byte[2],
            emptySender,
            Llcp(0x5001, Ebcdic("DATA")));

        var raw = DataQueueExecutor.ParseReadReply(reply);

        Assert.Null(raw.SenderInformation);
        Assert.Null(DataQueueSenderInfo.Parse(raw.SenderInformation));

        var entry = DataQueueEntry.FromRaw(raw);
        Assert.IsNotType<KeyedDataQueueEntry>(entry);
        Assert.Null(entry.SenderInfo);
        Assert.Equal("DATA", entry.GetString(37));
    }

    [Fact]
    public void ParseReadReplyHandlesZeroLengthEntry()
    {
        byte[] emptySender = new byte[36];
        Array.Fill(emptySender, (byte)0x40);

        byte[] reply = Reply(
            DataQueueExecutor.ReadReplyId,
            new byte[2],
            emptySender,
            Llcp(0x5001, Array.Empty<byte>()));

        var raw = DataQueueExecutor.ParseReadReply(reply);

        Assert.NotNull(raw.Data);
        Assert.Empty(raw.Data);
        Assert.Null(raw.Key);
    }

    [Fact]
    public void ParseReadReplyRejectsTruncatedPacket()
    {
        byte[] truncated = new byte[30];
        Assert.Throws<InvalidOperationException>(() => DataQueueExecutor.ParseReadReply(truncated));
    }

    [Fact]
    public void ParseCommonReplyExtractsRcAndMessage()
    {
        byte[] reply = Reply(
            DataQueueExecutor.CommonReplyId,
            new byte[] { 0xF0, 0x01 },                    // rc = 0xF001
            Llcp(0x5003, Ebcdic("CPF9801 Object not found")));

        var (rc, messageBytes) = DataQueueExecutor.ParseCommonReply(reply);

        Assert.Equal(0xF001, rc);
        Assert.NotNull(messageBytes);
        Assert.StartsWith("CPF9801", SharpSeries.Encoding.CcsidConverter.GetString(37, messageBytes));
    }

    [Fact]
    public void ParseCommonReplyWithoutMessageReturnsNull()
    {
        byte[] reply = Reply(
            DataQueueExecutor.CommonReplyId,
            new byte[] { 0xF0, 0x00 });                   // rc = success, no message block

        var (rc, messageBytes) = DataQueueExecutor.ParseCommonReply(reply);

        Assert.Equal(DataQueueExecutor.RcSuccess, rc);
        Assert.Null(messageBytes);
    }

    [Fact]
    public void ParseAttributesReplyReadsAllFields()
    {
        byte[] reply = Reply(
            DataQueueExecutor.QueryAttributesReplyId,
            new byte[] { 0x00, 0x00 },                    // bytes 20-21 (unused)
            new byte[] { 0x00, 0x00, 0xFB, 0x80 },        // max entry length 0xFB80 = 64384
            new byte[] { 0xF1 },                          // saves sender information
            new byte[] { 0x02 },                          // queue type: keyed
            new byte[] { 0x00, 0x08 },                    // key length 8
            new byte[] { 0xF0 },                          // no force to auxiliary storage
            Ebcdic("Test queue".PadRight(50)));

        var parsed = DataQueueExecutor.ParseAttributesReply(reply);

        Assert.Equal(64384, parsed.MaxEntryLength);
        Assert.True(parsed.SavesSenderInformation);
        Assert.Equal(2, parsed.QueueType);
        Assert.Equal(8, parsed.KeyLength);
        Assert.False(parsed.ForcesToAuxiliaryStorage);
        Assert.Equal("Test queue", SharpSeries.Encoding.CcsidConverter.GetString(37, parsed.DescriptionBytes).TrimEnd());
    }

    [Fact]
    public void GetReplyIdReadsBytes18To19()
    {
        byte[] reply = Reply(DataQueueExecutor.ReadReplyId);
        Assert.Equal(DataQueueExecutor.ReadReplyId, DataQueueExecutor.GetReplyId(reply));
    }
}

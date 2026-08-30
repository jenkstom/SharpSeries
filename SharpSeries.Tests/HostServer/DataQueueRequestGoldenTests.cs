using SharpSeries.HostServer;
using Xunit;
using static SharpSeries.Tests.HostServer.HostServerSessionBaseTests;

namespace SharpSeries.Tests.HostServer;

/// <summary>
/// Byte-level golden tests for data queue request packets, hand-derived from the
/// JTOpen layouts (DQWriteDataStream, DQReadDataStream, DQRequestAttributesDataStream,
/// DQExchangeAttributesDataStream).
/// </summary>
public class DataQueueRequestGoldenTests
{
    private const ushort Server = DataQueueExecutor.DataQueueServerId; // 0xE007

    private static byte[] Name => DataQueueExecutor.EncodeObjectName("QTEST");
    private static byte[] Library => DataQueueExecutor.EncodeObjectName("MYLIB");

    [Fact]
    public void ExchangeAttributesDeclaresVersion1AndLevel0()
    {
        byte[] buffer = new byte[26];
        DataQueueExecutor.WriteExchangeAttributes(buffer, out int length);

        byte[] expected = Concat(
            new byte[] { 0x00, 0x00, 0x00, 0x1A },   // total length 26
            new byte[] { 0x00, 0x00 },               // header ID
            new byte[] { 0xE0, 0x07 },               // server ID
            new byte[] { 0x00, 0x00, 0x00, 0x00 },   // CS instance
            new byte[] { 0x00, 0x00, 0x00, 0x00 },   // correlation
            new byte[] { 0x00, 0x06 },               // template length 6
            new byte[] { 0x00, 0x00 },               // reqRep ID 0x0000
            new byte[] { 0x00, 0x00, 0x00, 0x01 },   // client version 1 (64K entries)
            new byte[] { 0x00, 0x00 });              // data stream level 0

        Assert.Equal(26, length);
        Assert.Equal(expected, buffer);
    }

    [Fact]
    public void QueryAttributesIsHeaderPlusQueueAndLibrary()
    {
        byte[] name = DataQueueExecutor.EncodeObjectName("QTEST");
        byte[] library = DataQueueExecutor.EncodeObjectName("MYLIB");

        byte[] buffer = new byte[40];
        DataQueueExecutor.WriteQueryAttributes(buffer, name, library, out int length);

        byte[] expected = Concat(
            new byte[] { 0x00, 0x00, 0x00, 0x28 },   // total length 40
            new byte[] { 0x00, 0x00 },
            new byte[] { 0xE0, 0x07 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x14 },               // template length 20
            new byte[] { 0x00, 0x01 },               // reqRep ID 0x0001
            Ebcdic("QTEST     "),
            Ebcdic("MYLIB     "));

        Assert.Equal(40, length);
        Assert.Equal(expected, buffer);
    }

    [Fact]
    public void WriteNonKeyedEntryHasF0KeyFlagAndEntryBlock()
    {
        byte[] entry = Ebcdic("HI"); // C8 C9

        byte[] buffer = new byte[70_000];
        DataQueueExecutor.WriteEntry(buffer, Name, Library, key: null, entry, out int length);

        byte[] expected = Concat(
            new byte[] { 0x00, 0x00, 0x00, 0x32 },   // 48 + 2 = 50
            new byte[] { 0x00, 0x00 },
            new byte[] { 0xE0, 0x07 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x16 },               // template length 22
            new byte[] { 0x00, 0x05 },               // reqRep ID 0x0005
            Ebcdic("QTEST     "),
            Ebcdic("MYLIB     "),
            new byte[] { 0xF0 },                     // non-keyed
            new byte[] { 0xF1 },                     // want reply
            new byte[] { 0x00, 0x00, 0x00, 0x08 },   // entry LLCP: 6 + 2
            new byte[] { 0x50, 0x01 },               // entry data code point
            entry);

        Assert.Equal(50, length);
        Assert.Equal(expected, buffer.AsSpan(0, length).ToArray());
    }

    [Fact]
    public void WriteKeyedEntryAppendsKeyBlockAfterEntry()
    {
        byte[] entry = Ebcdic("HI");
        byte[] key = Ebcdic("K1"); // D2 F1

        byte[] buffer = new byte[70_000];
        DataQueueExecutor.WriteEntry(buffer, Name, Library, key, entry, out int length);

        byte[] expected = Concat(
            new byte[] { 0x00, 0x00, 0x00, 0x3A },   // 54 + 2 + 2 = 58
            new byte[] { 0x00, 0x00 },
            new byte[] { 0xE0, 0x07 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x16 },
            new byte[] { 0x00, 0x05 },
            Ebcdic("QTEST     "),
            Ebcdic("MYLIB     "),
            new byte[] { 0xF1 },                     // keyed
            new byte[] { 0xF1 },                     // want reply
            new byte[] { 0x00, 0x00, 0x00, 0x08 },
            new byte[] { 0x50, 0x01 },
            entry,
            new byte[] { 0x00, 0x00, 0x00, 0x08 },   // key LLCP: 6 + 2
            new byte[] { 0x50, 0x02 },               // key code point
            key);

        Assert.Equal(58, length);
        Assert.Equal(expected, buffer.AsSpan(0, length).ToArray());
    }

    [Fact]
    public void ReadNonKeyedEncodesZeroSearchNoWaitNoPeek()
    {
        byte[] buffer = new byte[300];
        DataQueueExecutor.WriteRead(buffer, Name, Library, new byte[2], wait: 0, peek: false, key: null, out int length);

        byte[] expected = Concat(
            new byte[] { 0x00, 0x00, 0x00, 0x30 },   // total length 48
            new byte[] { 0x00, 0x00 },
            new byte[] { 0xE0, 0x07 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x1C },               // template length 28
            new byte[] { 0x00, 0x02 },               // reqRep ID 0x0002
            Ebcdic("QTEST     "),
            Ebcdic("MYLIB     "),
            new byte[] { 0xF0 },                     // non-keyed
            new byte[] { 0x00, 0x00 },               // search operand (unused)
            new byte[] { 0x00, 0x00, 0x00, 0x00 },   // wait = 0 seconds
            new byte[] { 0xF0 });                    // peek = false

        Assert.Equal(48, length);
        Assert.Equal(expected, buffer.AsSpan(0, length).ToArray());
    }

    [Fact]
    public void ReadKeyedEncodesSearchWaitForeverAndPeek()
    {
        byte[] key = Ebcdic("K1");

        byte[] buffer = new byte[300];
        DataQueueExecutor.WriteRead(buffer, Name, Library, Ebcdic("EQ"), wait: -1, peek: true, key, out int length);

        byte[] expected = Concat(
            new byte[] { 0x00, 0x00, 0x00, 0x38 },   // 54 + 2 = 56
            new byte[] { 0x00, 0x00 },
            new byte[] { 0xE0, 0x07 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x1C },
            new byte[] { 0x00, 0x02 },
            Ebcdic("QTEST     "),
            Ebcdic("MYLIB     "),
            new byte[] { 0xF1 },                     // keyed
            Ebcdic("EQ"),                            // search operand
            new byte[] { 0xFF, 0xFF, 0xFF, 0xFF },   // wait = -1 (indefinitely)
            new byte[] { 0xF1 },                     // peek = true
            new byte[] { 0x00, 0x00, 0x00, 0x08 },   // key LLCP
            new byte[] { 0x50, 0x02 },
            key);

        Assert.Equal(56, length);
        Assert.Equal(expected, buffer.AsSpan(0, length).ToArray());
    }
}

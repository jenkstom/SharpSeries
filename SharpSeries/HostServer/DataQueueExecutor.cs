// This file is a derivative work of JTOpen (DQDataStream.java, DQWriteDataStream.java,
// DQReadDataStream.java, DQReadNormalReplyDataStream.java, DQCommonReplyDataStream.java,
// DQRequestAttributesDataStream.java, DQRequestAttributesNormalReplyDataStream.java,
// DQExchangeAttributesDataStream.java).
// Original source: https://github.com/IBM/JTOpen
// Copyright (C) 1997-2003 International Business Machines Corporation and others.
// Licensed under the IBM Public License v1.0.
// This file has been modified from the original.

using System.Buffers.Binary;
using SharpSeries.Encoding;

namespace SharpSeries.HostServer;

/// <summary>
/// Result of parsing a "receive record from data queue" (normal) reply: the raw entry
/// data, the key for keyed queues, and the 36-byte sender information block when the
/// queue was created with SENDERINF(*YES).
/// </summary>
public sealed class RawDataQueueEntry
{
    /// <summary>The entry data bytes (may be empty, never null).</summary>
    public byte[] Data { get; init; } = Array.Empty<byte>();

    /// <summary>The key bytes for keyed queues; null for non-keyed reads.</summary>
    public byte[]? Key { get; init; }

    /// <summary>
    /// The 36-byte sender information block (job name, user name, job number, current
    /// user profile), or null when the queue does not save sender information.
    /// </summary>
    public byte[]? SenderInformation { get; init; }
}

/// <summary>
/// The low-level byte manipulation routines for speaking the native IBM i Data Queue
/// Host Server protocol (QZHQSSRV, service "as-dtaq").
/// Packet layouts are ported directly from JTOpen's DQ*DataStream classes: every
/// request is the standard 20-byte host-server header followed by a fixed template
/// (queue name, library, flags) and optional LLCP data blocks (entry 0x5001, key 0x5002).
/// This class mirrors the role <see cref="QueryExecutor"/> plays for the SQL protocol.
/// </summary>
public static class DataQueueExecutor
{
    /// <summary>Host-server identifier for the data queue server ("as-dtaq"). See JTOpen AS400Server.getServerId().</summary>
    public const ushort DataQueueServerId = 0xE007;

    // Request ReqRep IDs (JTOpen DQ*DataStream classes).
    public const ushort ExchangeAttributesRequestId = 0x0000;
    public const ushort QueryAttributesRequestId = 0x0001;
    public const ushort ReadRequestId = 0x0002;
    public const ushort WriteRequestId = 0x0005;

    // Reply ReqRep IDs (JTOpen DQ*ReplyDataStream classes).
    public const ushort ExchangeAttributesReplyId = 0x8000;
    public const ushort QueryAttributesReplyId = 0x8001;
    public const ushort CommonReplyId = 0x8002;
    public const ushort ReadReplyId = 0x8003;

    // LLCP parameter code points within request/reply bodies.
    private const ushort EntryDataCodePoint = 0x5001;
    private const ushort KeyCodePoint = 0x5002;

    // Server return codes (JTOpen BaseDataQueueImplRemote.buildException()).
    public const int RcSuccess = 0xF000;
    public const int RcNoData = 0xF006;

    // EBCDIC zone digits used for boolean flags in templates ('0' = 0xF0, '1' = 0xF1).
    private const byte EbcdicFalse = 0xF0;
    private const byte EbcdicTrue = 0xF1;

    // EBCDIC space: an all-spaces sender information block means "no sender information".
    private const byte EbcdicSpace = 0x40;

    /// <summary>
    /// Encodes an object (queue/library) name for a request template: upper-cased,
    /// EBCDIC (CCSID 37), exactly 10 bytes space-padded. Accepts the special values
    /// *LIBL and *CURLIB for libraries.
    /// </summary>
    public static byte[] EncodeObjectName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Object name cannot be blank.", nameof(name));

        string upper = name.ToUpperInvariant().Trim();
        if (upper.Length > 10)
            throw new ArgumentException($"Object name '{name}' exceeds the 10-character system limit.", nameof(name));

        return CcsidConverter.GetBytes(37, upper.PadRight(10, ' '));
    }

    /// <summary>
    /// Formats the exchange client/server attributes request: must be the first data
    /// request sent on a fresh data queue server connection. Declares client version 1
    /// ("we support 64K data queue entries") and data stream level 0.
    /// </summary>
    public static void WriteExchangeAttributes(Memory<byte> buffer, out int length)
    {
        length = 26;
        HostServerSessionBase.WriteHostServerEnvelope(buffer, DataQueueServerId, (uint)length, templateLength: 6, ExchangeAttributesRequestId, correlation: 0);

        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 1); // Client version: 1 = 64K entries supported
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(24, 2), 0); // Client data stream level: always 0
    }

    /// <summary>
    /// Formats a query attributes request for a queue.
    /// </summary>
    public static void WriteQueryAttributes(Memory<byte> buffer, byte[] name, byte[] library, out int length)
    {
        length = 40;
        HostServerSessionBase.WriteHostServerEnvelope(buffer, DataQueueServerId, (uint)length, templateLength: 20, QueryAttributesRequestId, correlation: 0);

        WriteQueueAndLibrary(buffer.Span, name, library);
    }

    /// <summary>
    /// Formats an "add record to data queue" (write) request. A null key writes to a
    /// non-keyed (FIFO/LIFO) queue; otherwise the entry is written to a keyed queue.
    /// </summary>
    public static void WriteEntry(Memory<byte> buffer, byte[] name, byte[] library, byte[]? key, byte[] entry, out int length)
    {
        length = (key == null ? 48 : 54) + entry.Length + (key?.Length ?? 0);
        HostServerSessionBase.WriteHostServerEnvelope(buffer, DataQueueServerId, (uint)length, templateLength: 22, WriteRequestId, correlation: 0);

        WriteQueueAndLibrary(buffer.Span, name, library);

        buffer.Span[40] = key == null ? EbcdicFalse : EbcdicTrue;
        buffer.Span[41] = EbcdicTrue; // Want a reply.

        // Entry data block (0x5001).
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(42, 4), (uint)(6 + entry.Length));
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(46, 2), EntryDataCodePoint);
        entry.CopyTo(buffer.Span.Slice(48, entry.Length));

        if (key != null)
        {
            // Key block (0x5002).
            int offset = 48 + entry.Length;
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)(6 + key.Length));
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset + 4, 2), KeyCodePoint);
            key.CopyTo(buffer.Span.Slice(offset + 6, key.Length));
        }
    }

    /// <summary>
    /// Formats a "receive record from data queue" (read or peek) request.
    /// The search operand is the 2-byte EBCDIC operator (EQ/NE/LT/LE/GT/GE) for keyed
    /// reads, or two zero bytes for non-keyed reads. The wait time is a 32-bit value:
    /// 0 = do not wait, N = wait up to N seconds, -1 (0xFFFFFFFF) = wait indefinitely.
    /// </summary>
    public static void WriteRead(Memory<byte> buffer, byte[] name, byte[] library, byte[] search, int wait, bool peek, byte[]? key, out int length)
    {
        if (search.Length != 2)
            throw new ArgumentException("Search operand must be exactly 2 bytes.", nameof(search));

        length = key == null ? 48 : 54 + key.Length;
        HostServerSessionBase.WriteHostServerEnvelope(buffer, DataQueueServerId, (uint)length, templateLength: 28, ReadRequestId, correlation: 0);

        WriteQueueAndLibrary(buffer.Span, name, library);

        buffer.Span[40] = key == null ? EbcdicFalse : EbcdicTrue;
        search.CopyTo(buffer.Span.Slice(41, 2));
        BinaryPrimitives.WriteInt32BigEndian(buffer.Span.Slice(43, 4), wait);
        buffer.Span[47] = peek ? EbcdicTrue : EbcdicFalse;

        if (key != null)
        {
            // Key block (0x5002).
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(48, 4), (uint)(6 + key.Length));
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(52, 2), KeyCodePoint);
            key.CopyTo(buffer.Span.Slice(54, key.Length));
        }
    }

    /// <summary>
    /// Reads the ReqRep ID from a reply packet (bytes 18-19 of the standard header).
    /// </summary>
    public static ushort GetReplyId(byte[] reply)
        => BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(18, 2));

    /// <summary>
    /// Parses a common (write/attributes/exchange-attributes) reply: the return code
    /// at offset 20 and the optional EBCDIC message block that follows.
    /// </summary>
    public static (int Rc, byte[]? MessageBytes) ParseCommonReply(byte[] reply)
    {
        int rc = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(20, 2));

        byte[]? messageBytes = null;
        if (reply.Length > 22)
        {
            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(22, 4)) - 6;
            if (length > 0 && 28 + length <= reply.Length)
            {
                messageBytes = new byte[length];
                Array.Copy(reply, 28, messageBytes, 0, length);
            }
        }

        return (rc, messageBytes);
    }

    /// <summary>
    /// Parses a read (normal) reply: the 36-byte sender information block at offset 22
    /// (all EBCDIC spaces when the queue does not save sender information) followed by
    /// optional LLCP blocks carrying the entry data (0x5001) and key (0x5002).
    /// </summary>
    public static RawDataQueueEntry ParseReadReply(byte[] reply)
    {
        if (reply.Length < 58)
            throw new InvalidOperationException($"Data queue read reply is truncated ({reply.Length} bytes).");

        byte[]? senderInformation = null;
        if (reply[22] != EbcdicSpace)
        {
            senderInformation = new byte[36];
            Array.Copy(reply, 22, senderInformation, 0, 36);
        }

        byte[]? entry = null;
        byte[]? key = null;

        // Walk the LLCP chain that follows the sender information area.
        int offset = 58;
        while (offset < reply.Length - 6)
        {
            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(offset, 4));
            if (length < 6) break;

            ushort codePoint = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(offset + 4, 2));
            if (codePoint == EntryDataCodePoint)
            {
                entry = new byte[length - 6];
                Array.Copy(reply, offset + 6, entry, 0, length - 6);
            }
            else if (codePoint == KeyCodePoint)
            {
                key = new byte[length - 6];
                Array.Copy(reply, offset + 6, key, 0, length - 6);
            }

            offset += length;
        }

        return new RawDataQueueEntry
        {
            Data = entry ?? Array.Empty<byte>(),
            Key = key,
            SenderInformation = senderInformation,
        };
    }

    /// <summary>
    /// Parses a query attributes (normal) reply: max entry length, flags, queue type,
    /// key length, and the 50-byte EBCDIC text description.
    /// </summary>
    public static (int MaxEntryLength, bool SavesSenderInformation, byte QueueType, int KeyLength, bool ForcesToAuxiliaryStorage, byte[] DescriptionBytes) ParseAttributesReply(byte[] reply)
    {
        if (reply.Length < 81)
            throw new InvalidOperationException($"Data queue attributes reply is truncated ({reply.Length} bytes).");

        int maxEntryLength = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(22, 4));
        bool savesSenderInformation = reply[26] == EbcdicTrue;
        byte queueType = (byte)(reply[27] & 0x0F); // 0 = FIFO, 1 = LIFO, 2 = KEYED
        int keyLength = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(28, 2));
        bool forcesToAuxiliaryStorage = reply[30] == EbcdicTrue;

        byte[] descriptionBytes = new byte[50];
        Array.Copy(reply, 31, descriptionBytes, 0, 50);

        return (maxEntryLength, savesSenderInformation, queueType, keyLength, forcesToAuxiliaryStorage, descriptionBytes);
    }

    /// <summary>
    /// Writes the 10-byte queue name and library into the template at offsets 20 and 30.
    /// </summary>
    private static void WriteQueueAndLibrary(Span<byte> span, byte[] name, byte[] library)
    {
        name.CopyTo(span.Slice(20, 10));
        library.CopyTo(span.Slice(30, 10));
    }
}

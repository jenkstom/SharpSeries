// This file is a derivative work of JTOpen (BaseDataQueueImplRemote.java).
// Original source: https://github.com/IBM/JTOpen
// Copyright (C) 1997-2003 International Business Machines Corporation and others.
// Licensed under the IBM Public License v1.0.
// This file has been modified from the original.

using SharpSeries.DataQueues;
using SharpSeries.Logging;

namespace SharpSeries.HostServer;

/// <summary>
/// Manages the conversation with the IBM i Data Queue Host Server (QZHQSSRV,
/// service "as-dtaq"). Sign-on is inherited from <see cref="HostServerSessionBase"/>;
/// on top of that this class performs the mandatory exchange-attributes handshake
/// and issues write/read/peek/query-attributes requests via <see cref="DataQueueExecutor"/>.
/// </summary>
public class DataQueueConnectionManager : HostServerSessionBase
{
    protected override ushort ServerId => DataQueueExecutor.DataQueueServerId;
    protected override string ServiceName => "as-dtaq";

    // IBM's host server port documentation assigns 8474 to as-dtaq (9474 for TLS).
    // Note: JTOpen's PortMapper default table claims 8472 for the data queue server,
    // which contradicts IBM's documentation; the mapper resolves the real port at
    // runtime anyway, so this fallback only matters if the mapper is unreachable.
    protected override int FallbackPort => 8474;

    /// <summary>
    /// Connects and authenticates to the data queue server, then performs the
    /// exchange-attributes handshake that must be the first data request on a new
    /// server job (see JTOpen BaseDataQueueImplRemote.open()).
    /// </summary>
    public override async Task ConnectAndAuthenticateAsync(string host, string user, string password, CancellationToken cancellationToken = default)
    {
        await base.ConnectAndAuthenticateAsync(host, user, password, cancellationToken);
        await ExchangeAttributesAsync(cancellationToken);
    }

    /// <summary>
    /// Exchanges client/server attributes with the server job: declares support for
    /// 64K queue entries (client version 1).
    /// </summary>
    public async Task ExchangeAttributesAsync(CancellationToken cancellationToken = default)
    {
        byte[] packet = new byte[26];
        DataQueueExecutor.WriteExchangeAttributes(packet, out int length);
        await SendPacketAsync(packet.AsMemory(0, length), cancellationToken);

        var reply = await ReceiveReplyWithBodyAsync(DataQueueExecutor.ExchangeAttributesReplyId, cancellationToken)
            ?? throw new InvalidOperationException("Data queue server dropped the connection during the exchange-attributes handshake.");

        if (DataQueueExecutor.GetReplyId(reply) == DataQueueExecutor.CommonReplyId)
        {
            var (rc, messageBytes) = DataQueueExecutor.ParseCommonReply(reply);
            throw DataQueueException.Create(rc, messageBytes, "exchange attributes");
        }
    }

    /// <summary>
    /// Writes an entry to a data queue. A null key writes to a non-keyed (FIFO/LIFO)
    /// queue; a key writes to a keyed queue.
    /// </summary>
    public async Task WriteAsync(byte[] name, byte[] library, byte[]? key, byte[] entry, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(DataQueueConnectionManager)}] Write {entry.Length} bytes to data queue.");

        byte[] packet = new byte[70_000 + (key?.Length ?? 0)];
        DataQueueExecutor.WriteEntry(packet, name, library, key, entry, out int length);
        await SendPacketAsync(packet.AsMemory(0, length), cancellationToken);

        var reply = await ReceiveReplyWithBodyAsync(DataQueueExecutor.CommonReplyId, cancellationToken)
            ?? throw new InvalidOperationException("Data queue server dropped the connection during write.");

        if (DataQueueExecutor.GetReplyId(reply) != DataQueueExecutor.CommonReplyId)
            throw new InvalidOperationException($"Unexpected data queue write reply: 0x{DataQueueExecutor.GetReplyId(reply):X4}.");

        var (rc, messageBytes) = DataQueueExecutor.ParseCommonReply(reply);
        if (rc != DataQueueExecutor.RcSuccess)
            throw DataQueueException.Create(rc, messageBytes, "write");
    }

    /// <summary>
    /// Reads (or peeks) an entry from a data queue. Returns null when the queue holds
    /// no matching entry within the wait period. The search operand is the 2-byte
    /// EBCDIC operator for keyed reads, or two zero bytes for non-keyed reads.
    /// </summary>
    public async Task<RawDataQueueEntry?> ReadAsync(byte[] name, byte[] library, byte[] search, int wait, bool peek, byte[]? key, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(DataQueueConnectionManager)}] {(peek ? "Peek" : "Read")} (wait={wait}) from data queue.");

        byte[] packet = new byte[54 + 256];
        DataQueueExecutor.WriteRead(packet, name, library, search, wait, peek, key, out int length);
        await SendPacketAsync(packet.AsMemory(0, length), cancellationToken);

        var reply = await ReceiveReplyWithBodyAsync(DataQueueExecutor.ReadReplyId, cancellationToken)
            ?? throw new InvalidOperationException("Data queue server dropped the connection during read.");

        ushort replyId = DataQueueExecutor.GetReplyId(reply);
        if (replyId == DataQueueExecutor.ReadReplyId)
            return DataQueueExecutor.ParseReadReply(reply);

        if (replyId == DataQueueExecutor.CommonReplyId)
        {
            var (rc, messageBytes) = DataQueueExecutor.ParseCommonReply(reply);
            if (rc == DataQueueExecutor.RcNoData)
            {
                Db2Logger.Info($"[{nameof(DataQueueConnectionManager)}] No entry on data queue.");
                return null;
            }
            throw DataQueueException.Create(rc, messageBytes, peek ? "peek" : "read");
        }

        throw new InvalidOperationException($"Unexpected data queue read reply: 0x{replyId:X4}.");
    }

    /// <summary>
    /// Retrieves the attributes of a data queue.
    /// </summary>
    public async Task<DataQueueAttributes> GetAttributesAsync(byte[] name, byte[] library, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(DataQueueConnectionManager)}] Querying data queue attributes.");

        byte[] packet = new byte[40];
        DataQueueExecutor.WriteQueryAttributes(packet, name, library, out int length);
        await SendPacketAsync(packet.AsMemory(0, length), cancellationToken);

        var reply = await ReceiveReplyWithBodyAsync(DataQueueExecutor.QueryAttributesReplyId, cancellationToken)
            ?? throw new InvalidOperationException("Data queue server dropped the connection during query attributes.");

        ushort replyId = DataQueueExecutor.GetReplyId(reply);
        if (replyId == DataQueueExecutor.QueryAttributesReplyId)
        {
            var parsed = DataQueueExecutor.ParseAttributesReply(reply);
            return new DataQueueAttributes(
                parsed.MaxEntryLength,
                parsed.KeyLength,
                parsed.SavesSenderInformation,
                parsed.ForcesToAuxiliaryStorage,
                SharpSeries.Encoding.CcsidConverter.GetString(37, parsed.DescriptionBytes).TrimEnd(),
                (DataQueueType)parsed.QueueType);
        }

        if (replyId == DataQueueExecutor.CommonReplyId)
        {
            var (rc, messageBytes) = DataQueueExecutor.ParseCommonReply(reply);
            throw DataQueueException.Create(rc, messageBytes, "query attributes");
        }

        throw new InvalidOperationException($"Unexpected data queue attributes reply: 0x{replyId:X4}.");
    }
}

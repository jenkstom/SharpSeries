// This file is a derivative work of JTOpen (DatabaseConnection.java) and JTOpenLite (HostServerConnection.java, SignonConnection.java).
// Original source: https://github.com/IBM/JTOpen
// Copyright (C) 2011-2012 International Business Machines Corporation and others.
// Licensed under the IBM Public License v1.0.
// This file has been modified from the original.

using System.Buffers.Binary;
using SharpSeries.Encoding;
using SharpSeries.Logging;

namespace SharpSeries.HostServer;

/// <summary>
/// Manages the low-level SQL conversation with the IBM i Database Host Server (QZDASOINIT).
/// Connection establishment, the Server Mapper lookup, and the cryptographic sign-on
/// sequence are handled by <see cref="HostServerSessionBase"/>; this class layers the
/// DRDA-style SQL requests (statements, cursors, transactions) on top.
/// </summary>
public class HostServerConnectionManager : HostServerSessionBase
{
    /// <summary>Host-server identifier for the SQL database server ("as-database").</summary>
    public const ushort DatabaseServerId = 0xE004;

    // Counters to ensure unique IDs for statements and cursors per connection session
    private int _statementCounter;
    private string? _lastCursorName;
    private int _lastRpbId;

    protected override ushort ServerId => DatabaseServerId;
    protected override string ServiceName => "as-database";
    protected override int FallbackPort => 8471;

    /// <summary>
    /// Gets the name of the most recently opened server-side cursor.
    /// Useful for closing the cursor during cleanup.
    /// </summary>
    public string? LastCursorName => _lastCursorName;

    /// <summary>
    /// Executes the full IBM i Host Server connection and authentication sequence.
    /// Involves consulting the Port Mapper, establishing a socket, exchanging seeds,
    /// hashing passwords, and verifying credentials.
    /// The <paramref name="port"/> parameter is retained for API compatibility; the
    /// Server Mapper decides the actual port.
    /// </summary>
    public Task ConnectAndAuthenticateAsync(string host, int port, string user, string password, CancellationToken cancellationToken = default)
        => ConnectAndAuthenticateAsync(host, user, password, cancellationToken);

    /// <summary>
    /// Issues a hard COMMIT against the active connection.
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        byte[] p = new byte[40];
        WriteHostServerEnvelope(p, 40, 0x1807); // 0x1807 Action: Commit
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(20, 4), 0x80000000);
        if (_stream != null) await _stream.WriteAsync(p, cancellationToken);

        await ReceiveReplyAsync(0x2800, cancellationToken); // Await confirmation
    }

    /// <summary>
    /// Issues a hard ROLLBACK against the active connection.
    /// </summary>
    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        byte[] p = new byte[40];
        WriteHostServerEnvelope(p, 40, 0x1808); // 0x1808 Action: Rollback
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(20, 4), 0x80000000);
        if (_stream != null) await _stream.WriteAsync(p, cancellationToken);

        await ReceiveReplyAsync(0x2800, cancellationToken);
    }

    /// <summary>
    /// Executes a non-query SQL command (INSERT/UPDATE/DELETE).
    /// Orchestrates multiple DRDA network exchanges silently.
    /// </summary>
    public async Task<int> ExecuteSqlAsync(string sql, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] ExecuteSqlAsync called for SQL: {sql}");

        // Uniquely identify this statement to the database processor
        int id = ++_statementCounter;
        string stmtName = $"S{id:D6}";
        string cursorName = $"C{id:D6}";
        int rpbId = id;

        // Step 1: Create Request Parameter Block (RPB) (0x1D00)
        byte[] p0 = new byte[8192];
        QueryExecutor.WriteCreateRpb(p0, rpbId, stmtName, cursorName, out int len0);
        if (_stream != null) await _stream.WriteAsync(p0.AsMemory(0, len0), cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken);

        // Step 2: Prepare and Execute (0x180D)
        // Compiles and immediately executes the provided SQL.
        byte[] p1 = new byte[65536];
        QueryExecutor.WritePrepareAndExecute(p1, rpbId, sql, stmtName, out int len1);
        if (_stream != null) await _stream.WriteAsync(p1.AsMemory(0, len1), cancellationToken);

        // Listen for the combined result envelope
        var reply = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);

        if (reply != null)
        {
            ushort rcClass = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(34, 2));
            int rc = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(36, 4));

            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Execute reply: class={rcClass}, rc={rc}");

            if (rcClass != 0 && rc < 0)
            {
                // Execution bombed on the server side - automatically reel back open locks
                await RollbackAsync(cancellationToken);
                throw new InvalidOperationException($"SQL Execute failed. Return code class: {rcClass}, return code: {rc}");
            }

            // In AutoCommit paradigm, explicitly commit successful execution
            await CommitAsync(cancellationToken);

            // Delegate to executor tools to tease apart the network payload and discover row-count offsets
            int updateCount = QueryExecutor.ParseUpdateCount(reply);
            if (updateCount >= 0) return updateCount;
        }

        return -1;
    }

    /// <summary>
    /// Executes a SQL Select query, parses columns definitions, and fetches the first block of data rows.
    /// This method is the heavy lifter for the Db2DataReader.
    /// </summary>
    public async Task<QueryResult> OpenQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] OpenQueryAsync called for SQL: {sql}");

        var result = new QueryResult();
        int id = ++_statementCounter;
        string stmtName = $"S{id:D6}";
        string cursorName = $"C{id:D6}";
        int rpbId = id;

        // Step 1: Create Request Parameter Block (RPB) (0x1D00)
        byte[] p0 = new byte[8192];
        QueryExecutor.WriteCreateRpb(p0, rpbId, stmtName, cursorName, out int len0);
        if (_stream != null) await _stream.WriteAsync(p0.AsMemory(0, len0), cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken);

        // Step 2: Prepare & Describe (0x1803)
        byte[] p1 = new byte[65536];
        QueryExecutor.WritePrepareRequest(p1, rpbId, sql, stmtName, out int len1);
        if (_stream != null) await _stream.WriteAsync(p1.AsMemory(0, len1), cancellationToken);
        var reply1 = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);

        if (reply1 != null)
        {
            ushort rcClass = BinaryPrimitives.ReadUInt16BigEndian(reply1.AsSpan(34, 2));
            int rc = BinaryPrimitives.ReadInt32BigEndian(reply1.AsSpan(36, 4));

            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Prepare reply: class={rcClass}, rc={rc}");

            if (rcClass != 0 && rc < 0)
                throw new InvalidOperationException($"SQL Prepare failed. Return code class: {rcClass}, return code: {rc}");

            QueryExecutor.ParseFormatAndResults(reply1, result);
        }

        // Step 3: Open Describe Fetch (0x180E)
        byte[] p2 = new byte[65536];
        QueryExecutor.WriteOpenDescribeFetch(p2, rpbId, cursorName, out int len2);
        if (_stream != null) await _stream.WriteAsync(p2.AsMemory(0, len2), cancellationToken);
        var reply2 = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);

        if (reply2 != null)
        {
            ushort rcClass = BinaryPrimitives.ReadUInt16BigEndian(reply2.AsSpan(34, 2));
            int rc = BinaryPrimitives.ReadInt32BigEndian(reply2.AsSpan(36, 4));

            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Open reply: class={rcClass}, rc={rc}");

            QueryExecutor.ParseFormatAndResults(reply2, result);
        }

        _lastCursorName = cursorName;
        _lastRpbId = rpbId;

        return result;
    }

    /// <summary>
    /// Formats a manual cursor-close packet.
    /// Necessary because leaving cursors open will quickly leak resources on the IBM i side.
    /// </summary>
    public async Task CloseCursorAsync(string cursorName, CancellationToken cancellationToken = default)
    {
        // Cursor bindings are EBCDIC space-padded to exactly 10 characters length.
        var cursorNameBytes = CcsidConverter.GetBytes(37, cursorName.PadRight(10, ' ').Substring(0, 10));
        int cursorNameLL = 10 + cursorNameBytes.Length;

        int length = 40 + cursorNameLL;
        byte[] p = new byte[length];

        // Frame envelope headers
        WriteHostServerEnvelope(p, (uint)length, 0x180A); // Action: Close Cursor

        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(20, 4), 0x80000000);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(24, 4), 0);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(28, 2), 1);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(30, 2), 1);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(32, 2), 0);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(34, 2), (ushort)_lastRpbId); // Connect action to the previous tracked RPB
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(36, 2), 0);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(38, 2), 1);

        int offset = 40;
        // Inject cursor ID block (0x380B)
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(offset, 4), (uint)cursorNameLL);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(offset + 4, 2), 0x380B);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(offset + 6, 2), 37);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(offset + 8, 2), (ushort)cursorNameBytes.Length);
        cursorNameBytes.CopyTo(p.AsSpan(offset + 10, cursorNameBytes.Length));

        if (_stream != null) await _stream.WriteAsync(p.AsMemory(0, length), cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken); // Block for closing acknowledgement
    }
}

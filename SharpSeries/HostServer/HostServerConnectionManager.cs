// This file is a derivative work of JTOpen (DatabaseConnection.java) and JTOpenLite (HostServerConnection.java, SignonConnection.java).
// Original source: https://github.com/IBM/JTOpen
// Copyright (C) 2011-2012 International Business Machines Corporation and others.
// Licensed under the IBM Public License v1.0.
// This file has been modified from the original.

using System.Net.Sockets;
using SharpSeries.Network;
using SharpSeries.Encoding;
using SharpSeries.Logging;
using System.Buffers.Binary;

namespace SharpSeries.HostServer;

public class HostServerConnectionManager
{
    private HostServerStream? _stream;
    
    private byte[] _clientSeed = new byte[8];
    private byte[] _serverSeed = new byte[8];
    private int _statementCounter;
    private string? _lastCursorName;
    private int _lastRpbId;

    public string? LastCursorName => _lastCursorName;

    public async Task ConnectAndAuthenticateAsync(string host, int port, string user, string password, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] Beginning IBM i Host Server connection sequence to {host}...");

        // 1. Ask Server Mapper (port 449) for the correct Database port (typically 8471)
        int dbPort = await ResolveDatabasePortAsync(host, cancellationToken);
        
        Db2Logger.Info($"[{nameof(HostServerConnectionManager)}] Server Mapper returned database port: {dbPort}");

        // 2. Connect to the Database port (8471/8476)
        _stream = new HostServerStream();
        await _stream.ConnectAsync(host, dbPort, cancellationToken);

        // 3. Handshake Step 1: Exchange Random Seeds
        Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Performing Step 1: Exchange Random Seeds (0x7001)");
        new Random().NextBytes(_clientSeed);
        await SendRandomSeedsRequestAsync(_clientSeed, cancellationToken);
        var reply7001 = await ReceiveReplyWithBodyAsync(0x7001, cancellationToken);
        int passwordLevel = 2; // Default to SHA-1 or higher
        if (reply7001 != null && reply7001.Length >= 32)
        {
            passwordLevel = reply7001[5];
            Array.Copy(reply7001, 24, _serverSeed, 0, 8);
            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Server password level: {passwordLevel}");
        }

        // 4. Handshake Step 2: Start Server Challenge (Authentication)
        Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Performing Step 2: Start Server Challenge (0x7002)");
        await SendStartServerChallengeAsync(user, password, _clientSeed, _serverSeed, passwordLevel, cancellationToken);
        var reply2800 = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);
        
        // A standard 0x2800 payload will contain 0x00000000 in bytes 20-23 for RCClassReturnCode
        // If the reply contains a failure or ends early we should halt
        if (reply2800 == null || reply2800.Length < 24)
        {
            throw new InvalidOperationException("Host Server dropped connection. Invalid password or User ID.");
        }
        int returnCode = BinaryPrimitives.ReadInt32BigEndian(reply2800.AsSpan(20, 4));
        if (returnCode != 0)
        {
            throw new InvalidOperationException($"Authentication failed. Server Return Code: {returnCode}");
        }
        
        Db2Logger.Info($"[{nameof(HostServerConnectionManager)}] Connection and Authentication successful over Host Server protocol.");
    }

    private async Task<int> ResolveDatabasePortAsync(string host, CancellationToken cancellationToken)
    {
        using var mapperStream = new HostServerStream();
        await mapperStream.ConnectAsync(host, 449, cancellationToken);
        
        // Emulate sending a Port Mapper request for 'as-database'
        Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Sending Server Mapper Request for QZDASOINIT (as-database)...");
        
        // EBCDIC "as-database" string in hex (mock buffer)
        byte[] request = new byte[] { 0x01, 0x00, 0x00, 0x11, 0x81, 0xA2, 0x60, 0x84, 0x81, 0xA3, 0x81, 0x82, 0x81, 0xA2, 0x85 };
        await mapperStream.WriteAsync(request, cancellationToken);
        
        byte[] buffer = new byte[256];
        int bytesRead = await mapperStream.ReadAsync(buffer, cancellationToken);
        
        // Just as mock for now, assume 8471 if nothing interesting is returned
        int port = 8471;
        
        if (bytesRead > 0)
        {
            try
            {
               // Simplified extraction of port from port mapper reply
               if(buffer[0] == 0x02 || buffer[0] == '+') 
               {
                   // Try to read ascii port string (e.g., "+8471")
                   var str = System.Text.Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim('+');
                   if (int.TryParse(str, out int p)) port = p;
               }
            }
            catch { }
        }
        
        return port;
    }

    private async Task SendRandomSeedsRequestAsync(byte[] clientSeed, CancellationToken cancellationToken)
    {
        // Total Length 28 bytes
        byte[] packet = new byte[28];
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0), 28); // Total Len
        packet[4] = 3; // Client Attributes (1=SHA-1, 2=pwdlvl 4, 3=both?) JTOpen lite uses 1, JTOpen uses 3. Let's use 3.
        packet[5] = 0; // Server Attributes
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(6), 0xE004); // Server ID DB
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(8), 0); // CS Instance
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(12), 1); // Correlator
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(16), 8); // Template Len (8 byte seed)
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(18), 0x7001); // ReqRep ID
        
        // Provide 8-byte client random seed at offset 20
        Array.Copy(clientSeed, 0, packet, 20, 8);
        
        if (_stream != null) await _stream.WriteAsync(packet, cancellationToken);
    }

    private async Task SendStartServerChallengeAsync(string user, string password, byte[] clientSeed, byte[] serverSeed, int passwordLevel, CancellationToken cancellationToken)
    {
        // 1. Prepare User ID (Attribute 0x1104)
        // IBM specifically requires UserID to be exactly 10 characters long, padded with spaces, upper-cased
        string paddedUser = (user.ToUpperInvariant() + "          ").Substring(0, 10);
        byte[] userEbcdicBytes = CcsidConverter.GetBytes(37, paddedUser);
        
        byte[] passHash;
        int encryptionType = 3;
        
        if (passwordLevel <= 1)
        {
            encryptionType = 1; // DES
            
            // Password must be max 10 characters, upper-cased for DES 
            string upperPassword = password.ToUpperInvariant();
            if (upperPassword.Length > 10) upperPassword = upperPassword.Substring(0, 10);
            
            // Prepend Q if numeric
            if (upperPassword.Length > 0 && char.IsDigit(upperPassword[0]))
            {
                upperPassword = "Q" + upperPassword;
            }
            if (upperPassword.Length > 10) upperPassword = upperPassword.Substring(0, 10);
            
            string paddedPass = (upperPassword + "          ").Substring(0, 10);
            byte[] passEbcdicBytes = CcsidConverter.GetBytes(37, paddedPass);

            passHash = SharpSeries.Security.DesPasswordEncryptor.EncryptPasswordDES(userEbcdicBytes, passEbcdicBytes, clientSeed, serverSeed);
        }
        else
        {
            encryptionType = 3; // SHA-1
            
            // User ID for Hash token MUST be strictly UTF-16 BE per the SHA1 authentication specification
            byte[] userHashBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(paddedUser);
            
            // Password MUST be stripped of trailing spaces and encoded in standard UTF-16 BE
            byte[] passBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(password.TrimEnd());
            
            // 2. Prepare Password Hash (Attribute 0x1105)
            byte[] sequence = { 0, 0, 0, 0, 0, 0, 0, 1 };
            
            using (var sha1 = System.Security.Cryptography.SHA1.Create())
            {
                var tokenInput = new byte[userHashBytes.Length + passBytes.Length];
                Array.Copy(userHashBytes, 0, tokenInput, 0, userHashBytes.Length);
                Array.Copy(passBytes, 0, tokenInput, userHashBytes.Length, passBytes.Length);
                byte[] token = sha1.ComputeHash(tokenInput);

                var subInput = new byte[token.Length + serverSeed.Length + clientSeed.Length + userHashBytes.Length + sequence.Length];
                int pos = 0;
                Array.Copy(token, 0, subInput, pos, token.Length); pos += token.Length;
                Array.Copy(serverSeed, 0, subInput, pos, serverSeed.Length); pos += serverSeed.Length;
                Array.Copy(clientSeed, 0, subInput, pos, clientSeed.Length); pos += clientSeed.Length;
                Array.Copy(userHashBytes, 0, subInput, pos, userHashBytes.Length); pos += userHashBytes.Length;
                Array.Copy(sequence, 0, subInput, pos, sequence.Length);
                
                passHash = sha1.ComputeHash(subInput);
            }
        }
        
        // 3. Calculate lengths
        int userAttrLen = 6 + userEbcdicBytes.Length;
        int passAttrLen = 6 + passHash.Length;
        int totalLength = 20 + 2 + userAttrLen + passAttrLen; // Header + Template + Attrs

        // 4. Build Packet
        byte[] packet = new byte[totalLength];
        
        // --- 20 Byte Envelope Header ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0), (uint)totalLength);
        packet[4] = 2; // Client Attributes
        packet[5] = 0; // Server Attributes
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(6), 0xE004); // Server ID DB
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(8), 0); // CS Instance
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(12), 2); // Correlator
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(16), 2); // Template Len
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(18), 0x7002); // ReqRep ID
        
        // --- 2 Byte Template ---
        packet[20] = (byte)encryptionType; 
        packet[21] = 1; // Send Reply
        
        int offset = 22;
        
        // --- LTV Attribute: 0x1105 Password ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(offset), (uint)passAttrLen);
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(offset + 4), 0x1105);
        Array.Copy(passHash, 0, packet, offset + 6, passHash.Length);
        offset += passAttrLen;

        // --- LTV Attribute: 0x1104 User ID ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(offset), (uint)userAttrLen);
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(offset + 4), 0x1104);
        Array.Copy(userEbcdicBytes, 0, packet, offset + 6, userEbcdicBytes.Length);
        
        if (_stream != null) await _stream.WriteAsync(packet, cancellationToken);
    }

    private async Task ReceiveReplyAsync(ushort expectedReplyCodePoint, CancellationToken cancellationToken)
    {
        await ReceiveReplyWithBodyAsync(expectedReplyCodePoint, cancellationToken);
    }

    private async Task<byte[]?> ReceiveReplyWithBodyAsync(ushort expectedReplyCodePoint, CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[1048576]; // 1MB buffer to support large query result payloads
        try
        {
            Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] Waiting for reply, expecting ReqRep ID 0x{expectedReplyCodePoint:X4}...");
            if (_stream == null) return null;
            
            int bytesRead = 0;
            // Read at least 4 bytes to get the length
            while (bytesRead < 4)
            {
                int r = await _stream.ReadAsync(buffer.AsMemory(bytesRead, 4 - bytesRead), cancellationToken);
                if (r == 0) break;
                bytesRead += r;
            }
            
            if (bytesRead >= 4)
            {
                int totalLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4));
                while (bytesRead < totalLength)
                {
                    int r = await _stream.ReadAsync(buffer.AsMemory(bytesRead, totalLength - bytesRead), cancellationToken);
                    if (r == 0) break;
                    bytesRead += r;
                }
            }
            
            if (bytesRead > 0)
            {
                Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] Received {bytesRead} bytes from server.");
                
                if (bytesRead >= 20)
                {
                    ushort repId = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(18, 2));
                    Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Reply ReqRep parsed: 0x{repId:X4}");
                    
                    var result = new byte[bytesRead];
                    Array.Copy(buffer, result, bytesRead);
                    return result;
                }
            }
            else
            {
                Db2Logger.Warn($"[{nameof(HostServerConnectionManager)}] Received 0 bytes (connection closed by server?)");
            }
        }
        catch (Exception ex)
        {
            Db2Logger.Error($"[{nameof(HostServerConnectionManager)}] Error receiving reply: {ex.Message}");
            throw;
        }
        return null;
    }

    public void Disconnect()
    {
        Db2Logger.Info($"[{nameof(HostServerConnectionManager)}] Disconnecting physical stream.");
        _stream?.Dispose();
        _stream = null;
    }

    private void WriteDummyHostServerEnvelope(Memory<byte> buffer, uint length, ushort reqRepId)
    {
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), length);
        buffer.Span[4] = 0; // Client Attributes
        buffer.Span[5] = 0; // Server Attributes
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004); // Server ID DB
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0); // CS Instance
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 3); // Correlator
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20); // Template Length (dummy payload length)
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), reqRepId); // ReqRep ID
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        byte[] p = new byte[40];
        WriteDummyHostServerEnvelope(p, 40, 0x1807);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(20, 4), 0x80000000);
        if (_stream != null) await _stream.WriteAsync(p, cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken);
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        byte[] p = new byte[40];
        WriteDummyHostServerEnvelope(p, 40, 0x1808);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(20, 4), 0x80000000);
        if (_stream != null) await _stream.WriteAsync(p, cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken);
    }

    public async Task<int> ExecuteSqlAsync(string sql, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] ExecuteSqlAsync called for SQL: {sql}");

        int id = ++_statementCounter;
        string stmtName = $"S{id:D6}";
        string cursorName = $"C{id:D6}";
        int rpbId = id;

        // Step 1: Create RPB (0x1D00) - fire-and-forget (no reply expected)
        byte[] p0 = new byte[8192];
        QueryExecutor.WriteCreateRpb(p0, rpbId, stmtName, cursorName, out int len0);
        if (_stream != null) await _stream.WriteAsync(p0.AsMemory(0, len0), cancellationToken);

        // Step 2: Prepare and Execute (0x180D) - prepares and executes SQL in one request
        byte[] p1 = new byte[65536];
        QueryExecutor.WritePrepareAndExecute(p1, rpbId, sql, stmtName, out int len1);
        if (_stream != null) await _stream.WriteAsync(p1.AsMemory(0, len1), cancellationToken);
        var reply = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);
        if (reply != null)
        {
            ushort rcClass = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(34, 2));
            int rc = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(36, 4));
            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Execute reply: class={rcClass}, rc={rc}");
            if (rcClass != 0 && rc < 0)
            {
                await RollbackAsync(cancellationToken);
                throw new InvalidOperationException($"SQL Execute failed. Return code class: {rcClass}, return code: {rc}");
            }

            await CommitAsync(cancellationToken);

            int updateCount = QueryExecutor.ParseUpdateCount(reply);

            if (updateCount >= 0) return updateCount;
        }

        return -1;
    }

    public async Task<QueryResult> OpenQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] OpenQueryAsync called for SQL: {sql}");
        
        var result = new QueryResult();
        int id = ++_statementCounter;
        string stmtName = $"S{id:D6}";
        string cursorName = $"C{id:D6}";
        int rpbId = id;

        // Step 1: Create RPB (0x1D00) - fire-and-forget (no reply expected, matching JTOpen)
        byte[] p0 = new byte[8192];
        QueryExecutor.WriteCreateRpb(p0, rpbId, stmtName, cursorName, out int len0);
        if (_stream != null) await _stream.WriteAsync(p0.AsMemory(0, len0), cancellationToken);

        // Step 2: Prepare & Describe (0x1803) - prepares SQL, returns column format (0x3812)
        byte[] p1 = new byte[65536];
        QueryExecutor.WritePrepareRequest(p1, rpbId, sql, stmtName, out int len1);
        if (_stream != null) await _stream.WriteAsync(p1.AsMemory(0, len1), cancellationToken);
        var reply1 = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);
        if (reply1 != null)
        {
            ushort rcClass = BinaryPrimitives.ReadUInt16BigEndian(reply1.AsSpan(34, 2));
            int rc = BinaryPrimitives.ReadInt32BigEndian(reply1.AsSpan(36, 4));
            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Prepare reply: class={rcClass}, rc={rc}");
            if (rcClass != 0)
                throw new InvalidOperationException($"SQL Prepare failed. Return code class: {rcClass}, return code: {rc}");
            QueryExecutor.ParseFormatAndResults(reply1, result);
        }

        // Step 3: Open Describe Fetch (0x180E) - opens cursor and fetches first data block (0x380E)
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

    public async Task CloseCursorAsync(string cursorName, CancellationToken cancellationToken = default)
    {
        var cursorNameBytes = CcsidConverter.GetBytes(37, cursorName.PadRight(10, ' ').Substring(0, 10));
        int cursorNameLL = 10 + cursorNameBytes.Length;

        int length = 40 + cursorNameLL;
        byte[] p = new byte[length];

        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(0, 4), (uint)length);
        p[4] = 0; p[5] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(6, 2), 0xE004);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(8, 4), 0);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(12, 4), 3);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(16, 2), 20);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(18, 2), 0x180A);

        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(20, 4), 0x80000000);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(24, 4), 0);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(28, 2), 1);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(30, 2), 1);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(32, 2), 0);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(34, 2), (ushort)_lastRpbId);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(36, 2), 0);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(38, 2), 1);

        int offset = 40;
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(offset, 4), (uint)cursorNameLL);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(offset + 4, 2), 0x380B);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(offset + 6, 2), 37);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(offset + 8, 2), (ushort)cursorNameBytes.Length);
        cursorNameBytes.CopyTo(p.AsSpan(offset + 10, cursorNameBytes.Length));

        if (_stream != null) await _stream.WriteAsync(p.AsMemory(0, length), cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken);
    }
}

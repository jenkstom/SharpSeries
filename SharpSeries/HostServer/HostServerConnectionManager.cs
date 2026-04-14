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

/// <summary>
/// Manages the low-level, physical TCP/IP connection to the IBM i Host Server (QZDASOINIT).
/// This class handles the complex DRDA (Distributed Relational Database Architecture) protocol,
/// including port mapping, cryptographic handshakes, and binary packet envelope construction.
/// </summary>
public class HostServerConnectionManager
{
    private HostServerStream? _stream;
    
    // Random seeds used for challenge/response authentication
    private byte[] _clientSeed = new byte[8];
    private byte[] _serverSeed = new byte[8];
    
    // Counters to ensure unique IDs for statements and cursors per connection session
    private int _statementCounter;
    private string? _lastCursorName;
    private int _lastRpbId;

    /// <summary>
    /// Gets the name of the most recently opened server-side cursor.
    /// Useful for closing the cursor during cleanup.
    /// </summary>
    public string? LastCursorName => _lastCursorName;

    /// <summary>
    /// Executes the full IBM i Host Server connection and authentication sequence.
    /// Involves consulting the Port Mapper, establishing a socket, exchanging seeds,
    /// hashing passwords, and verifying credentials.
    /// </summary>
    public async Task ConnectAndAuthenticateAsync(string host, int port, string user, string password, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] Beginning IBM i Host Server connection sequence to {host}...");

        // 1. Port Mapper Enquiry
        // IBM i uses a server mapper running on port 449 to dynamically assign ports to various services.
        // We ask where the "as-database" service (QZDASOINIT) is currently listening.
        int dbPort = await ResolveDatabasePortAsync(host, cancellationToken);
        
        Db2Logger.Info($"[{nameof(HostServerConnectionManager)}] Server Mapper returned database port: {dbPort}");

        // 2. Physical TCP Connection
        // Establish the main socket connection to the database server port (often 8471).
        _stream = new HostServerStream();
        await _stream.ConnectAsync(host, dbPort, cancellationToken);

        // 3. Handshake Step 1: Exchange Random Seeds
        // Both the client and server generate 8-byte random seeds.
        // These seeds are combined with the user and password to prevent replay attacks.
        Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Performing Step 1: Exchange Random Seeds (0x7001)");
        new Random().NextBytes(_clientSeed);
        await SendRandomSeedsRequestAsync(_clientSeed, cancellationToken);
        var reply7001 = await ReceiveReplyWithBodyAsync(0x7001, cancellationToken);
        
        int passwordLevel = 2; // Default to SHA-1 or higher if not specified
        if (reply7001 != null && reply7001.Length >= 32)
        {
            // The server tells us its maximum supported authentication level (DES vs SHA-1)
            passwordLevel = reply7001[5];
            // Extract the server's 8-byte seed from the packet body
            Array.Copy(reply7001, 24, _serverSeed, 0, 8);
            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Server password level: {passwordLevel}");
        }

        // 4. Handshake Step 2: Start Server Challenge
        // Send the hashed credentials back to the server.
        Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Performing Step 2: Start Server Challenge (0x7002)");
        await SendStartServerChallengeAsync(user, password, _clientSeed, _serverSeed, passwordLevel, cancellationToken);
        
        // Wait for the final authentication reply
        var reply2800 = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);
        
        // Check for valid response envelope
        if (reply2800 == null || reply2800.Length < 24)
        {
            throw new InvalidOperationException("Host Server dropped connection. Invalid password or User ID.");
        }
        
        // Inspect the Return Code mapped at byte 20. 0 means success.
        int returnCode = BinaryPrimitives.ReadInt32BigEndian(reply2800.AsSpan(20, 4));
        if (returnCode != 0)
        {
            throw new InvalidOperationException($"Authentication failed. Server Return Code: {returnCode}");
        }
        
        Db2Logger.Info($"[{nameof(HostServerConnectionManager)}] Connection and Authentication successful over Host Server protocol.");
    }

    /// <summary>
    /// Contacts the IBM i Server Mapper (port 449) to locate the "as-database" service port.
    /// </summary>
    private async Task<int> ResolveDatabasePortAsync(string host, CancellationToken cancellationToken)
    {
        using var mapperStream = new HostServerStream();
        await mapperStream.ConnectAsync(host, 449, cancellationToken);
        
        Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Sending Server Mapper Request for QZDASOINIT (as-database)...");
        
        // Raw EBCDIC query string asking for the database service port
        byte[] request = new byte[] { 0x01, 0x00, 0x00, 0x11, 0x81, 0xA2, 0x60, 0x84, 0x81, 0xA3, 0x81, 0x82, 0x81, 0xA2, 0x85 };
        await mapperStream.WriteAsync(request, cancellationToken);
        
        byte[] buffer = new byte[256];
        int bytesRead = await mapperStream.ReadAsync(buffer, cancellationToken);
        
        int port = 8471; // Safe fallback standard port
        
        if (bytesRead > 0)
        {
            try
            {
               // Simplistic parser: looks for ASCII port string (e.g., "+8471") returned by mapper
               if(buffer[0] == 0x02 || buffer[0] == '+') 
               {
                   var str = System.Text.Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim('+');
                   if (int.TryParse(str, out int p)) port = p;
               }
            }
            catch { }
        }
        
        return port;
    }

    /// <summary>
    /// Sends the initial 0x7001 handshake request to exchange cryptographic seeds.
    /// </summary>
    private async Task SendRandomSeedsRequestAsync(byte[] clientSeed, CancellationToken cancellationToken)
    {
        byte[] packet = new byte[28];
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0), 28); // Total Packet Length
        packet[4] = 3; // Client Attributes flags: Requesting SHA-1 support if available
        packet[5] = 0; // Server Attributes (empty on send)
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(6), 0xE004); // Server ID (0xE004 = Database)
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(8), 0); // CS Instance
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(12), 1); // Message Correlator 
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(16), 8); // Payload Length
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(18), 0x7001); // Request ID: Exchange random seeds
        
        // Inject the 8-byte client seed payload
        Array.Copy(clientSeed, 0, packet, 20, 8);
        
        if (_stream != null) await _stream.WriteAsync(packet, cancellationToken);
    }

    /// <summary>
    /// Computes the authentication token (DES or SHA1) and sends the 0x7002 Login Challenge packet.
    /// </summary>
    private async Task SendStartServerChallengeAsync(string user, string password, byte[] clientSeed, byte[] serverSeed, int passwordLevel, CancellationToken cancellationToken)
    {
        // 1. Prepare User ID (Attribute 0x1104)
        // IBM specifically requires UserID to be exactly 10 characters long, padded with spaces, and upper-cased
        string paddedUser = (user.ToUpperInvariant() + "          ").Substring(0, 10);
        byte[] userEbcdicBytes = CcsidConverter.GetBytes(37, paddedUser);
        
        byte[] passHash;
        int encryptionType = 3; // 3 implies SHA-1, 1 implies DES
        
        // Determine encryption method requested by server profile
        if (passwordLevel <= 1)
        {
            // --- Legacy DES Encryption Process ---
            encryptionType = 1;
            
            // Passwords must be max 10 length, uppercase. 
            string upperPassword = password.ToUpperInvariant();
            if (upperPassword.Length > 10) upperPassword = upperPassword.Substring(0, 10);
            
            // Legacy nuance: if password starts with a number, prepend 'Q'
            if (upperPassword.Length > 0 && char.IsDigit(upperPassword[0]))
            {
                upperPassword = "Q" + upperPassword;
            }
            if (upperPassword.Length > 10) upperPassword = upperPassword.Substring(0, 10);
            
            string paddedPass = (upperPassword + "          ").Substring(0, 10);
            byte[] passEbcdicBytes = CcsidConverter.GetBytes(37, paddedPass);

            // Delegate to the legacy DES routines ported from JTOpen
            passHash = SharpSeries.Security.DesPasswordEncryptor.EncryptPasswordDES(userEbcdicBytes, passEbcdicBytes, clientSeed, serverSeed);
        }
        else
        {
            // --- Modern SHA-1 Encryption Process ---
            encryptionType = 3;
            
            // Both IDs and Passwords must be encoded as strict UTF-16 Big Endian for SHA-1 hash inputs
            byte[] userHashBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(paddedUser);
            byte[] passBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(password.TrimEnd()); // Note: No padding on password
            
            // Sequence seed required for standard QZDASOINIT hash
            byte[] sequence = { 0, 0, 0, 0, 0, 0, 0, 1 };
            
            // Perform the multi-stage hashing sequence defined by DRDA Host server spec
            using (var sha1 = System.Security.Cryptography.SHA1.Create())
            {
                var tokenInput = new byte[userHashBytes.Length + passBytes.Length];
                Array.Copy(userHashBytes, 0, tokenInput, 0, userHashBytes.Length);
                Array.Copy(passBytes, 0, tokenInput, userHashBytes.Length, passBytes.Length);
                byte[] token = sha1.ComputeHash(tokenInput); // First phase token

                // Combine first phase token with connection seeds
                var subInput = new byte[token.Length + serverSeed.Length + clientSeed.Length + userHashBytes.Length + sequence.Length];
                int pos = 0;
                Array.Copy(token, 0, subInput, pos, token.Length); pos += token.Length;
                Array.Copy(serverSeed, 0, subInput, pos, serverSeed.Length); pos += serverSeed.Length;
                Array.Copy(clientSeed, 0, subInput, pos, clientSeed.Length); pos += clientSeed.Length;
                Array.Copy(userHashBytes, 0, subInput, pos, userHashBytes.Length); pos += userHashBytes.Length;
                Array.Copy(sequence, 0, subInput, pos, sequence.Length);
                
                passHash = sha1.ComputeHash(subInput); // Final authentication token
            }
        }
        
        // Format the structured logical lengths
        int userAttrLen = 6 + userEbcdicBytes.Length;
        int passAttrLen = 6 + passHash.Length;
        int totalLength = 20 + 2 + userAttrLen + passAttrLen;

        byte[] packet = new byte[totalLength];
        
        // --- Standard 20-Byte DRDA Packet Header ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0), (uint)totalLength);
        packet[4] = 2; // Client Attributes
        packet[5] = 0; // Server Attributes
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(6), 0xE004); // DB Server identifier
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(8), 0); // Instance
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(12), 2); // Correlator
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(16), 2); // Payload Len indicator
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(18), 0x7002); // ID: Request authentication
        
        // Header flags
        packet[20] = (byte)encryptionType; 
        packet[21] = 1; // Explicitly map a reply receipt
        
        int offset = 22;
        
        // --- Packet Attribute Structure: 0x1105 Password block ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(offset), (uint)passAttrLen);
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(offset + 4), 0x1105);
        Array.Copy(passHash, 0, packet, offset + 6, passHash.Length);
        offset += passAttrLen;

        // --- Packet Attribute Structure: 0x1104 User ID block ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(offset), (uint)userAttrLen);
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(offset + 4), 0x1104);
        Array.Copy(userEbcdicBytes, 0, packet, offset + 6, userEbcdicBytes.Length);
        
        if (_stream != null) await _stream.WriteAsync(packet, cancellationToken);
    }

    /// <summary>
    /// Helper to await a specific reply packet without returning a body payload.
    /// </summary>
    private async Task ReceiveReplyAsync(ushort expectedReplyCodePoint, CancellationToken cancellationToken)
    {
        await ReceiveReplyWithBodyAsync(expectedReplyCodePoint, cancellationToken);
    }

    /// <summary>
    /// Listens on the network stream and fully reads the next DRDA boundary packet into memory.
    /// Handles packet boundary chunking dynamically.
    /// </summary>
    private async Task<byte[]?> ReceiveReplyWithBodyAsync(ushort expectedReplyCodePoint, CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[1048576]; // Generous 1MB buffer: Resultsets from queries can easily reach hundred of KBs.
        try
        {
            Db2Logger.Trace($"[{nameof(HostServerConnectionManager)}] Waiting for reply, expecting ReqRep ID 0x{expectedReplyCodePoint:X4}...");
            if (_stream == null) return null;
            
            int bytesRead = 0;
            // Packet boundary framing logic: First 4 bytes explicitly dictate the entire payload length.
            while (bytesRead < 4)
            {
                int r = await _stream.ReadAsync(buffer.AsMemory(bytesRead, 4 - bytesRead), cancellationToken);
                if (r == 0) break; // Stream ended
                bytesRead += r;
            }
            
            // Loop again until we receive the absolute number of bytes declared in the header 
            // (TCP doesn't guarantee full packet delivery in single read)
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

    /// <summary>
    /// Forcibly kills the network stream, closing the connection.
    /// </summary>
    public void Disconnect()
    {
        Db2Logger.Info($"[{nameof(HostServerConnectionManager)}] Disconnecting physical stream.");
        _stream?.Dispose();
        _stream = null;
    }

    /// <summary>
    /// Utility to format standard 20-byte headers for transaction commands.
    /// </summary>
    private void WriteDummyHostServerEnvelope(Memory<byte> buffer, uint length, ushort reqRepId)
    {
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), length);
        buffer.Span[4] = 0;
        buffer.Span[5] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0);
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 3);
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20); // Dummy template length
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), reqRepId); 
    }

    /// <summary>
    /// Issues a hard COMMIT against the active connection.
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        byte[] p = new byte[40];
        WriteDummyHostServerEnvelope(p, 40, 0x1807); // 0x1807 Action: Commit
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
        WriteDummyHostServerEnvelope(p, 40, 0x1808); // 0x1808 Action: Rollback
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

        // Step 1: Create Request Parameter Block (0x1D00)
        byte[] p0 = new byte[8192];
        QueryExecutor.WriteCreateRpb(p0, rpbId, stmtName, cursorName, out int len0);
        if (_stream != null) await _stream.WriteAsync(p0.AsMemory(0, len0), cancellationToken);
        await ReceiveReplyAsync(0x2800, cancellationToken);

        // Step 2: Prepare & Describe (0x1803)
        // Send SQL text. Expect a reply formatted as 0x3812 (Column Types and Definitions).
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
                
            // Extract the metadata so we know exactly how to parse returning data shapes
            QueryExecutor.ParseFormatAndResults(reply1, result);
        }

        // Step 3: Open Describe Fetch (0x180E) 
        // Activate cursor logic on the server and fetch the first swath of data in one shot.
        byte[] p2 = new byte[65536];
        QueryExecutor.WriteOpenDescribeFetch(p2, rpbId, cursorName, out int len2);
        if (_stream != null) await _stream.WriteAsync(p2.AsMemory(0, len2), cancellationToken);
        var reply2 = await ReceiveReplyWithBodyAsync(0x2800, cancellationToken);
        
        if (reply2 != null)
        {
            ushort rcClass = BinaryPrimitives.ReadUInt16BigEndian(reply2.AsSpan(34, 2));
            int rc = BinaryPrimitives.ReadInt32BigEndian(reply2.AsSpan(36, 4));
            
            Db2Logger.Debug($"[{nameof(HostServerConnectionManager)}] Open reply: class={rcClass}, rc={rc}");
            
            // Extract the actual tabular raw binary data into memory.
            QueryExecutor.ParseFormatAndResults(reply2, result);
        }

        // Record the states so we can properly close this specific cursor on disposal
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
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(0, 4), (uint)length);
        p[4] = 0; p[5] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(6, 2), 0xE004);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(8, 4), 0);
        BinaryPrimitives.WriteUInt32BigEndian(p.AsSpan(12, 4), 3);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(16, 2), 20);
        BinaryPrimitives.WriteUInt16BigEndian(p.AsSpan(18, 2), 0x180A); // Action: Close Cursor

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

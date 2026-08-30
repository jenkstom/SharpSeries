// This file is a derivative work of JTOpen (AS400Server.java, PortMapper.java)
// and JTOpenLite (HostServerConnection.java, SignonConnection.java, PortMapper.java).
// Original source: https://github.com/IBM/JTOpen
// Copyright (C) 1997-2024 International Business Machines Corporation and others.
// Licensed under the IBM Public License v1.0.
// This file has been modified from the original.

using System.Buffers.Binary;
using SharpSeries.Encoding;
using SharpSeries.Logging;
using SharpSeries.Network;

namespace SharpSeries.HostServer;

/// <summary>
/// Common plumbing shared by every IBM i Host Server session (database, data queue, ...).
/// Handles the Server Mapper lookup on port 449, the physical TCP connection, and the
/// generic host-server sign-on sequence (random seed exchange 0x7001 followed by the
/// start-server challenge 0x7002 with DES or SHA-1 password encryption).
/// Subclasses identify the service they talk to via <see cref="ServerId"/> and
/// <see cref="ServiceName"/>, and add their own request/reply protocol on top.
/// </summary>
public abstract class HostServerSessionBase
{
    // Well-known port of the IBM i Server Mapper daemon (QZSOMAPD).
    private const int ServerMapperPort = 449;

    protected HostServerStream? _stream;

    // Random seeds used for challenge/response authentication
    private byte[] _clientSeed = new byte[8];
    private byte[] _serverSeed = new byte[8];

    /// <summary>
    /// The host-server identifier stamped into every packet header for this service
    /// (e.g. 0xE004 for the SQL database server, 0xE007 for the data queue server).
    /// See JTOpen AS400Server.getServerId().
    /// </summary>
    protected abstract ushort ServerId { get; }

    /// <summary>
    /// The service name registered with the Server Mapper (e.g. "as-database", "as-dtaq").
    /// </summary>
    protected abstract string ServiceName { get; }

    /// <summary>
    /// The standard port for this service, used when the Server Mapper is unreachable.
    /// </summary>
    protected abstract int FallbackPort { get; }

    /// <summary>
    /// Builds the Server Mapper request for a service: the plain ASCII service name.
    /// The mapper replies with '+' (0x2B) followed by the 4-byte big-endian port number.
    /// See JTOpen AS400PortMapDS / JTOpenLite PortMapper.
    /// </summary>
    public static byte[] BuildServiceMapperRequest(string serviceName)
        => System.Text.Encoding.ASCII.GetBytes(serviceName);

    /// <summary>
    /// Executes the full IBM i Host Server connection and authentication sequence:
    /// consults the Port Mapper, establishes a socket, exchanges random seeds (0x7001),
    /// and answers the start-server challenge (0x7002) with hashed credentials.
    /// </summary>
    public virtual async Task ConnectAndAuthenticateAsync(string host, string user, string password, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{GetType().Name}] Beginning IBM i Host Server connection sequence to {host} for service '{ServiceName}'...");

        // 1. Port Mapper Enquiry
        // IBM i uses a server mapper running on port 449 to dynamically assign ports to services.
        int port = await ResolveServicePortAsync(host, ServiceName, FallbackPort, cancellationToken);

        Db2Logger.Info($"[{GetType().Name}] Server Mapper returned {ServiceName} port: {port}");

        // 2. Physical TCP Connection
        _stream = new HostServerStream();
        await _stream.ConnectAsync(host, port, cancellationToken);

        // 3. Handshake Step 1: Exchange Random Seeds
        // Both the client and server generate 8-byte random seeds.
        // These seeds are combined with the user and password to prevent replay attacks.
        Db2Logger.Debug($"[{GetType().Name}] Performing Step 1: Exchange Random Seeds (0x7001)");
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
            Db2Logger.Debug($"[{GetType().Name}] Server password level: {passwordLevel}");
        }

        // 4. Handshake Step 2: Start Server Challenge
        // Send the hashed credentials back to the server.
        Db2Logger.Debug($"[{GetType().Name}] Performing Step 2: Start Server Challenge (0x7002)");
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

        Db2Logger.Info($"[{GetType().Name}] Connection and Authentication successful over Host Server protocol ({ServiceName}).");
    }

    /// <summary>
    /// Contacts the IBM i Server Mapper (port 449) to locate a service port.
    /// Falls back to the service's standard port if the mapper cannot be reached or
    /// returns something unexpected.
    /// </summary>
    protected async Task<int> ResolveServicePortAsync(string host, string serviceName, int fallbackPort, CancellationToken cancellationToken)
    {
        try
        {
            using var mapperStream = new HostServerStream();
            await mapperStream.ConnectAsync(host, ServerMapperPort, cancellationToken);

            Db2Logger.Debug($"[{GetType().Name}] Sending Server Mapper Request for service '{serviceName}'...");

            // The request is the plain ASCII service name; the mapper handles one request per socket.
            await mapperStream.WriteAsync(BuildServiceMapperRequest(serviceName), cancellationToken);

            // The reply is 5 bytes: '+' (0x2B) followed by the 4-byte big-endian port number.
            byte[] buffer = new byte[5];
            int bytesRead = 0;
            while (bytesRead < 5)
            {
                int r = await mapperStream.ReadAsync(buffer.AsMemory(bytesRead, 5 - bytesRead), cancellationToken);
                if (r == 0) break; // Stream ended
                bytesRead += r;
            }

            if (bytesRead == 5 && buffer[0] == 0x2B)
            {
                int mapped = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(1, 4));
                if (mapped > 0)
                    return mapped;
            }

            Db2Logger.Warn($"[{GetType().Name}] Unexpected Server Mapper reply. Falling back to standard port {fallbackPort}.");
        }
        catch (Exception ex)
        {
            Db2Logger.Warn($"[{GetType().Name}] Server Mapper unavailable ({ex.Message}). Falling back to standard port {fallbackPort}.");
        }

        return fallbackPort;
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
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(6), ServerId);
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

            // Sequence seed required for standard host server hash
            byte[] sequence = { 0, 0, 0, 0, 0, 0, 0, 1 };

            // Perform the multi-stage hashing sequence defined by the host server spec
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

        // --- Standard 20-Byte Host Server Packet Header ---
        BinaryPrimitives.WriteUInt32BigEndian(packet.AsSpan(0), (uint)totalLength);
        packet[4] = 2; // Client Attributes
        packet[5] = 0; // Server Attributes
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(6), ServerId);
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
    protected async Task ReceiveReplyAsync(ushort expectedReplyCodePoint, CancellationToken cancellationToken)
    {
        await ReceiveReplyWithBodyAsync(expectedReplyCodePoint, cancellationToken);
    }

    /// <summary>
    /// Listens on the network stream and fully reads the next host-server boundary packet into memory.
    /// Handles packet boundary chunking dynamically.
    /// </summary>
    protected async Task<byte[]?> ReceiveReplyWithBodyAsync(ushort expectedReplyCodePoint, CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[1048576]; // Generous 1MB buffer: result sets and queue entries can reach ~64KB.
        try
        {
            Db2Logger.Trace($"[{GetType().Name}] Waiting for reply, expecting ReqRep ID 0x{expectedReplyCodePoint:X4}...");
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
                Db2Logger.Trace($"[{GetType().Name}] Received {bytesRead} bytes from server.");

                if (bytesRead >= 20)
                {
                    ushort repId = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(18, 2));
                    Db2Logger.Debug($"[{GetType().Name}] Reply ReqRep parsed: 0x{repId:X4}");

                    var result = new byte[bytesRead];
                    Array.Copy(buffer, result, bytesRead);
                    return result;
                }
            }
            else
            {
                Db2Logger.Warn($"[{GetType().Name}] Received 0 bytes (connection closed by server?)");
            }
        }
        catch (Exception ex)
        {
            Db2Logger.Error($"[{GetType().Name}] Error receiving reply: {ex.Message}");
            throw;
        }
        return null;
    }

    /// <summary>
    /// Forcibly kills the network stream, closing the connection.
    /// </summary>
    public void Disconnect()
    {
        Db2Logger.Info($"[{GetType().Name}] Disconnecting physical stream.");
        _stream?.Dispose();
        _stream = null;
    }

    /// <summary>
    /// Writes a raw packet to the server stream if the session is connected.
    /// </summary>
    protected Task SendPacketAsync(Memory<byte> packet, CancellationToken cancellationToken)
    {
        if (_stream == null) throw new InvalidOperationException("Session is not connected.");
        return _stream.WriteAsync(packet, cancellationToken);
    }

    /// <summary>
    /// Utility to format the standard 20-byte host-server packet header for simple
    /// template-only requests (e.g. transaction commands).
    /// </summary>
    protected void WriteHostServerEnvelope(Memory<byte> buffer, uint length, ushort reqRepId)
    {
        WriteHostServerEnvelope(buffer, ServerId, length, 20, reqRepId, correlation: 3);
    }

    /// <summary>
    /// Formats the standard 20-byte host-server packet header shared by every request:
    /// total length (4), header ID/flags (2), server ID (2), CS instance (4),
    /// correlation (4), template length (2), and request/reply ID (2).
    /// </summary>
    public static void WriteHostServerEnvelope(Memory<byte> buffer, ushort serverId, uint length, ushort templateLength, ushort reqRepId, uint correlation)
    {
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), length);
        buffer.Span[4] = 0;
        buffer.Span[5] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), serverId);
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0);
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), correlation);
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), templateLength);
        BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), reqRepId);
    }
}

using System.Net.Sockets;
using System.Net.Security;
using SharpSeries.Logging;
using System.Buffers.Binary;

namespace SharpSeries.Network;

public class HostServerStream : IDisposable
{
    private TcpClient _client;
    private Stream _stream;
    private string _host = string.Empty;
    private int _port;

    public HostServerStream()
    {
        _client = new TcpClient();
        _stream = Stream.Null;
    }

    public async Task ConnectAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        _host = host;
        _port = port;
        Db2Logger.Trace($"[{nameof(HostServerStream)}] Attempting TCP connection to {_host}:{_port}");
        
        await _client.ConnectAsync(host, port, cancellationToken);
        _stream = _client.GetStream();
        
        if (_client.Client.RemoteEndPoint != null)
        {
            Db2Logger.Trace($"[{nameof(HostServerStream)}] Physical TCP connection established to {_client.Client.RemoteEndPoint}");
        }
    }

    public async Task UpgradeToTlsAsync(string targetHost, CancellationToken cancellationToken = default)
    {
        Db2Logger.Trace($"[{nameof(HostServerStream)}] Upgrading connection to TLS/SSL for target: {targetHost}");
        var sslStream = new SslStream(_stream, false);
        await sslStream.AuthenticateAsClientAsync(targetHost);
        _stream = sslStream;
    }

    public async Task WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (Db2Logger.Level <= Db2LogLevel.Trace)
        {
            Db2Logger.Trace($"[{nameof(HostServerStream)}] --> SEND to {_host}:{_port} [{buffer.Length} bytes]: {Convert.ToHexString(buffer.Span)}");
        }
        await _stream.WriteAsync(buffer, cancellationToken);
    }

    public async Task<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int bytesRead = await _stream.ReadAsync(buffer, cancellationToken);
        
        if (bytesRead > 0 && Db2Logger.Level <= Db2LogLevel.Trace)
        {
            Db2Logger.Trace($"[{nameof(HostServerStream)}] <-- RECV from {_host}:{_port} [{bytesRead} bytes]: {Convert.ToHexString(buffer.Span.Slice(0, bytesRead))}");
        }
        
        return bytesRead;
    }

    public void Dispose()
    {
        _stream?.Dispose();
        _client?.Dispose();
    }
}

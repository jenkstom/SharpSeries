#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
using System.Data;
using System.Data.Common;
using SharpSeries.HostServer;

namespace SharpSeries.Data;

public class Db2Connection : DbConnection
{
    private Db2ConnectionStringBuilder _connectionStringBuilder = new();
    private ConnectionState _state = ConnectionState.Closed;
    private bool _readOnly;

    internal HostServerConnectionManager? HostServerManager { get; private set; }

    public override string ConnectionString
    {
        get => _connectionStringBuilder.ConnectionString;
        set => _connectionStringBuilder.ConnectionString = value ?? string.Empty;
    }

    public override string Database => _connectionStringBuilder.Database;

    public override string DataSource => _connectionStringBuilder.Server;

    public override string ServerVersion => "V7R4M0";

    public override ConnectionState State => _state;

    public bool IsReadOnly => _readOnly || _connectionStringBuilder.ReadOnly;

    public Db2Connection()
    {
    }

    public Db2Connection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    public void SetReadOnly(bool readOnly)
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");

        if (!readOnly && _connectionStringBuilder.ReadOnly)
            throw new InvalidOperationException("Cannot disable read-only on a connection opened with 'Read Only=true'.");

        _readOnly = readOnly;
    }

    public override void ChangeDatabase(string databaseName)
    {
        _connectionStringBuilder.Database = databaseName;
    }

    public override void Close()
    {
        if (_state == ConnectionState.Open && HostServerManager != null)
        {
            SharpSeries.Pool.ConnectionPool.ReturnConnection(ConnectionString, HostServerManager);
            HostServerManager = null;
            _state = ConnectionState.Closed;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
        }
    }

    public override void Open()
    {
        OpenAsync().GetAwaiter().GetResult();
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state != ConnectionState.Closed)
            return;

        HostServerManager = await SharpSeries.Pool.ConnectionPool.GetConnectionAsync(
            ConnectionString,
            _connectionStringBuilder.Server, 
            _connectionStringBuilder.Port, 
            _connectionStringBuilder.UserID, 
            _connectionStringBuilder.Password, 
            cancellationToken);

        _state = ConnectionState.Open;
        OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        // Begin DRDA transaction logic
        return new Db2Transaction(this, isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
        return new Db2Command { Connection = this };
    }
}

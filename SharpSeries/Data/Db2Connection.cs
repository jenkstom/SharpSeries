#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
using System.Data;
using System.Data.Common;
using SharpSeries.HostServer;

namespace SharpSeries.Data;

/// <summary>
/// Represents an open connection to an IBM i Db2 database.
/// This class is the primary entry point for establishing a session with the host server.
/// It extends the standard ADO.NET <see cref="DbConnection"/> to provide integration
/// with existing .NET data access patterns.
/// </summary>
public class Db2Connection : DbConnection
{
    // The builder responsible for parsing and managing the connection string properties.
    private Db2ConnectionStringBuilder _connectionStringBuilder = new();
    
    // Tracks the current state of the connection (e.g., Closed, Open).
    private ConnectionState _state = ConnectionState.Closed;
    
    // Internal flag to track if the connection is currently explicitly set to read-only mode during this session.
    private bool _readOnly;

    /// <summary>
    /// The underlying manager handling the physical network connection and DRDA protocol
    /// communication with the IBM i Host Server. This is internal so the provider's
    /// components (like DbCommand) can use it, but it remains hidden from end-users.
    /// </summary>
    internal HostServerConnectionManager? HostServerManager { get; private set; }

    /// <summary>
    /// Gets or sets the string used to open a Db2 database.
    /// Uses <see cref="Db2ConnectionStringBuilder"/> under the hood to parse and store values.
    /// </summary>
    public override string ConnectionString
    {
        get => _connectionStringBuilder.ConnectionString;
        set => _connectionStringBuilder.ConnectionString = value ?? string.Empty;
    }

    /// <summary>
    /// Gets the name of the current database or the database to be used after a connection is opened.
    /// </summary>
    public override string Database => _connectionStringBuilder.Database;

    /// <summary>
    /// Gets the name of the database server to which to connect.
    /// </summary>
    public override string DataSource => _connectionStringBuilder.Server;

    /// <summary>
    /// Gets the version of the IBM i Db2 instance. 
    /// Note: Currently hardcoded to V7R4M0 as a placeholder/default.
    /// </summary>
    public override string ServerVersion => "V7R4M0";

    /// <summary>
    /// Gets the current state of the connection.
    /// </summary>
    public override ConnectionState State => _state;

    /// <summary>
    /// Indicates whether the connection is in read-only mode.
    /// A connection can be read-only if specified in the connection string 
    /// OR explicitly set via <see cref="SetReadOnly(bool)"/>.
    /// </summary>
    public bool IsReadOnly => _readOnly || _connectionStringBuilder.ReadOnly;

    /// <summary>
    /// Initializes a new instance of the <see cref="Db2Connection"/> class.
    /// </summary>
    public Db2Connection()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Db2Connection"/> class with the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection used to open the database.</param>
    public Db2Connection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Toggles the connection's read-only mode dynamically for the current session.
    /// </summary>
    /// <param name="readOnly">True to enforce read-only access; false otherwise.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the connection is closed, or if attempting to disable read-only mode
    /// on a connection that was fundamentally configured as read-only in its connection string.
    /// </exception>
    public void SetReadOnly(bool readOnly)
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");

        // If the connection string dictated 'Read Only=true', we cannot override it to false later.
        if (!readOnly && _connectionStringBuilder.ReadOnly)
            throw new InvalidOperationException("Cannot disable read-only on a connection opened with 'Read Only=true'.");

        _readOnly = readOnly;
    }

    /// <summary>
    /// Changes the current database for an open connection.
    /// Currently just updates the builder state.
    /// </summary>
    /// <param name="databaseName">The name of the database to use.</param>
    public override void ChangeDatabase(string databaseName)
    {
        _connectionStringBuilder.Database = databaseName;
    }

    /// <summary>
    /// Closes the connection to the database.
    /// Instead of dropping the physical connection, this returns the underlying 
    /// <see cref="HostServerConnectionManager"/> back to the <see cref="SharpSeries.Pool.ConnectionPool"/>
    /// to be reused by future connections with the same connection string.
    /// </summary>
    public override void Close()
    {
        if (_state == ConnectionState.Open && HostServerManager != null)
        {
            // Return connection to the shared pool
            SharpSeries.Pool.ConnectionPool.ReturnConnection(ConnectionString, HostServerManager);
            HostServerManager = null;
            
            // Update ADO.NET state and notify listeners
            _state = ConnectionState.Closed;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
        }
    }

    /// <summary>
    /// Synchronously opens a connection to a database.
    /// This delegates entirely to the asynchronous OpenAsync method.
    /// </summary>
    public override void Open()
    {
        OpenAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Asynchronously opens a connection to a database.
    /// This requests an established connection from the <see cref="SharpSeries.Pool.ConnectionPool"/>
    /// and updates the connection state.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        // Don't try to open an already opened connection
        if (_state != ConnectionState.Closed)
            return;

        // Retrieve a physical connection from the pool (or create a new one if necessary)
        HostServerManager = await SharpSeries.Pool.ConnectionPool.GetConnectionAsync(
            ConnectionString,
            _connectionStringBuilder.Server, 
            _connectionStringBuilder.Port, 
            _connectionStringBuilder.UserID, 
            _connectionStringBuilder.Password, 
            cancellationToken);

        // Mark as open and notify listeners
        _state = ConnectionState.Open;
        OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
    }

    /// <summary>
    /// Starts a database transaction.
    /// </summary>
    /// <param name="isolationLevel">Specifies the isolation level for the transaction.</param>
    /// <returns>An object representing the new transaction.</returns>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        // Encapsulate DRDA transaction start logic
        return new Db2Transaction(this, isolationLevel);
    }

    /// <summary>
    /// Creates and returns a <see cref="DbCommand"/> object associated with the connection.
    /// </summary>
    /// <returns>A <see cref="Db2Command"/> object.</returns>
    protected override DbCommand CreateDbCommand()
    {
        return new Db2Command { Connection = this };
    }
}

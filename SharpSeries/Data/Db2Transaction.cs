using System.Data;
using System.Data.Common;

namespace SharpSeries.Data;

/// <summary>
/// Represents a database transaction to be made against an IBM i Db2 database.
/// Wraps the DRDA-level commit and rollback protocols into the ADO.NET abstraction.
/// By default, operations on Db2 Host Server are auto-committed unless specifically batched under this class.
/// </summary>
public sealed class Db2Transaction : DbTransaction
{
    private Db2Connection _connection;
    private IsolationLevel _isolationLevel;
    private bool _completed; // Prevents double-resolution of transaction

    /// <summary>
    /// Initializes a new instance of the Db2Transaction class.
    /// Internal constructor, accessible only via DbConnection.BeginTransaction().
    /// </summary>
    public Db2Transaction(DbConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection as Db2Connection ?? throw new ArgumentNullException(nameof(connection));
        _isolationLevel = isolationLevel;
    }

    /// <summary>
    /// Gets the DbConnection object associated with the transaction, or null if the transaction is no longer valid.
    /// </summary>
    protected override DbConnection? DbConnection => _connection;

    /// <summary>
    /// Specifies the IsolationLevel for this transaction.
    /// Note: Currently stored but not heavily enforced on the host server packets in this MVP version.
    /// </summary>
    public override IsolationLevel IsolationLevel => _isolationLevel;

    /// <summary>
    /// Commits the database transaction.
    /// Issues a DRDA Action 0x1807 to flush and persist pending SQL operations to disk.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the transaction has already been resolved.</exception>
    public override void Commit()
    {
        if (_completed)
            throw new InvalidOperationException("Transaction has already been committed or rolled back.");

        // Block on the asynchronous network call to ensure the commit arrived and was acknowledged
        _connection.HostServerManager?.CommitAsync().GetAwaiter().GetResult();
        _completed = true; // Mark resolved
    }

    /// <summary>
    /// Rolls back a transaction from a pending state.
    /// Issues a DRDA Action 0x1808 to reverse pending modifications and release database locks.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the transaction has already been resolved.</exception>
    public override void Rollback()
    {
        if (_completed)
            throw new InvalidOperationException("Transaction has already been committed or rolled back.");

        _connection.HostServerManager?.RollbackAsync().GetAwaiter().GetResult();
        _completed = true; // Mark resolved
    }
}

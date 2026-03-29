using System.Data;
using System.Data.Common;

namespace SharpSeries.Data;

public sealed class Db2Transaction : DbTransaction
{
    private Db2Connection _connection;
    private IsolationLevel _isolationLevel;
    private bool _completed;

    public Db2Transaction(DbConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection as Db2Connection ?? throw new ArgumentNullException(nameof(connection));
        _isolationLevel = isolationLevel;
    }

    protected override DbConnection? DbConnection => _connection;

    public override IsolationLevel IsolationLevel => _isolationLevel;

    public override void Commit()
    {
        if (_completed)
            throw new InvalidOperationException("Transaction has already been committed or rolled back.");

        _connection.HostServerManager?.CommitAsync().GetAwaiter().GetResult();
        _completed = true;
    }

    public override void Rollback()
    {
        if (_completed)
            throw new InvalidOperationException("Transaction has already been committed or rolled back.");

        _connection.HostServerManager?.RollbackAsync().GetAwaiter().GetResult();
        _completed = true;
    }
}

#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
using System.Data;
using System.Data.Common;

namespace SharpSeries.Data;

public class Db2Command : DbCommand
{
    private string _commandText = string.Empty;
    private int _commandTimeout = 30;
    private CommandType _commandType = CommandType.Text;
    private DbConnection? _dbConnection;
    private DbTransaction? _dbTransaction;
    private bool _designTimeVisible = true;
    private UpdateRowSource _updateRowSource = UpdateRowSource.Both;
    private Db2ParameterCollection _parameters = new();

    public Db2Command() { }

    public Db2Command(string cmdText)
    {
        _commandText = cmdText;
    }

    public Db2Command(string cmdText, DbConnection connection)
    {
        _commandText = cmdText;
        _dbConnection = connection;
    }

    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value;
    }

    public override int CommandTimeout
    {
        get => _commandTimeout;
        set => _commandTimeout = value;
    }

    public override CommandType CommandType
    {
        get => _commandType;
        set => _commandType = value;
    }

    public override bool DesignTimeVisible
    {
        get => _designTimeVisible;
        set => _designTimeVisible = value;
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => _updateRowSource;
        set => _updateRowSource = value;
    }

    protected override DbConnection? DbConnection
    {
        get => _dbConnection;
        set => _dbConnection = value;
    }

    protected override DbParameterCollection DbParameterCollection => _parameters;

    protected override DbTransaction? DbTransaction
    {
        get => _dbTransaction;
        set => _dbTransaction = value;
    }

    public override void Cancel()
    {
        // Issue an interrupt socket command
    }

    public override int ExecuteNonQuery()
    {
        if (_dbConnection is not Db2Connection db2conn || db2conn.HostServerManager == null)
            throw new InvalidOperationException("Connection not set or closed");

        return db2conn.HostServerManager.ExecuteSqlAsync(_commandText).GetAwaiter().GetResult();
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        if (reader.Read() && reader.FieldCount > 0)
        {
            return reader.GetValue(0);
        }
        return null;
    }

    public override void Prepare()
    {
        // Issue PRPSQLSTT early
    }

    protected override DbParameter CreateDbParameter()
    {
        return new Db2Parameter();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (_dbConnection is not Db2Connection db2conn || db2conn.HostServerManager == null)
            throw new InvalidOperationException("Connection not set or closed");

        var res = db2conn.HostServerManager.OpenQueryAsync(_commandText).GetAwaiter().GetResult();
        return new Db2DataReader(res, db2conn.HostServerManager, db2conn.HostServerManager.LastCursorName);
    }
}

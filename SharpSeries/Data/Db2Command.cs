#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;

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

        if (db2conn.IsReadOnly)
            throw new InvalidOperationException("Connection is read-only. ExecuteNonQuery is not allowed.");

        string sql = ResolveSql();
        return db2conn.HostServerManager.ExecuteSqlAsync(sql).GetAwaiter().GetResult();
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

        string sql = ResolveSql();
        var res = db2conn.HostServerManager.OpenQueryAsync(sql).GetAwaiter().GetResult();
        return new Db2DataReader(res, db2conn.HostServerManager, db2conn.HostServerManager.LastCursorName);
    }

    private string ResolveSql()
    {
        string sql = _commandText;

        if (_commandType == CommandType.StoredProcedure)
        {
            if (_parameters.Count > 0)
                sql = $"CALL {sql}({string.Join(", ", Enumerable.Range(0, _parameters.Count).Select(_ => "?"))})";
            else
                sql = $"CALL {sql}()";
        }

        if (_parameters.Count == 0)
            return sql;

        return SubstituteParameters(sql);
    }

    private string SubstituteParameters(string sql)
    {
        var result = new StringBuilder(sql.Length + _parameters.Count * 32);
        int paramIndex = 0;
        bool inString = false;

        for (int i = 0; i < sql.Length; i++)
        {
            char c = sql[i];

            if (c == '\'')
            {
                if (inString && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    result.Append("''");
                    i++;
                    continue;
                }
                inString = !inString;
                result.Append(c);
            }
            else if (c == '?' && !inString)
            {
                if (paramIndex < _parameters.Count)
                {
                    result.Append(FormatParameterValue(_parameters[paramIndex]));
                    paramIndex++;
                }
                else
                {
                    result.Append(c);
                }
            }
            else
            {
                result.Append(c);
            }
        }

        return result.ToString();
    }

    private static string FormatParameterValue(DbParameter param)
    {
        if (param.Value == null || param.Value == DBNull.Value)
            return "NULL";

        var val = param.Value;

        if (val is string s)
            return "'" + s.Replace("'", "''") + "'";
        if (val is int i)
            return i.ToString(CultureInfo.InvariantCulture);
        if (val is long l)
            return l.ToString(CultureInfo.InvariantCulture);
        if (val is short sh)
            return sh.ToString(CultureInfo.InvariantCulture);
        if (val is decimal d)
            return d.ToString(CultureInfo.InvariantCulture);
        if (val is double dbl)
            return dbl.ToString("R", CultureInfo.InvariantCulture);
        if (val is float f)
            return f.ToString("R", CultureInfo.InvariantCulture);
        if (val is bool b)
            return b ? "'1'" : "'0'";
        if (val is DateTime dt)
        {
            if (param.DbType == DbType.Date)
                return "'" + dt.ToString("yyyy-MM-dd") + "'";
            if (param.DbType == DbType.Time)
                return "'" + dt.ToString("HH.mm.ss") + "'";
            return "'" + dt.ToString("yyyy-MM-dd-HH.mm.ss.ffffff") + "'";
        }
        if (val is byte[] bytes)
            return "X'" + Convert.ToHexString(bytes) + "'";
        if (val is Guid g)
            return "'" + g.ToString() + "'";
        if (val is byte by)
            return by.ToString(CultureInfo.InvariantCulture);

        return "'" + val.ToString()!.Replace("'", "''") + "'";
    }
}

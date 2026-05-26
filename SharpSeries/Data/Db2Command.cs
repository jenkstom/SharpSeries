#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;

namespace SharpSeries.Data;

/// <summary>
/// Represents a SQL statement or stored procedure to execute against an IBM i Db2 database.
/// Provides methods to execute commands that return single values, result sets, or just row counts.
/// </summary>
public class Db2Command : DbCommand
{
    private string _commandText = string.Empty;
    private int _commandTimeout = 30; // Default timeout of 30 seconds for command execution
    private CommandType _commandType = CommandType.Text; // Default to raw SQL text
    private DbConnection? _dbConnection;
    private DbTransaction? _dbTransaction;
    private bool _designTimeVisible = true;
    private UpdateRowSource _updateRowSource = UpdateRowSource.Both;
    
    // Internal collection holding parameters associated with the command
    private Db2ParameterCollection _parameters = new();

    /// <summary>
    /// Initializes a new, empty instance of the <see cref="Db2Command"/> class.
    /// </summary>
    public Db2Command() { }

    /// <summary>
    /// Initializes a new instance of the <see cref="Db2Command"/> class with the text of the query.
    /// </summary>
    /// <param name="cmdText">The text of the query.</param>
    public Db2Command(string cmdText)
    {
        _commandText = cmdText;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Db2Command"/> class with the text of the query and a connection.
    /// </summary>
    /// <param name="cmdText">The text of the query.</param>
    /// <param name="connection">A <see cref="DbConnection"/> that represents the connection to an instance of IBM i Db2.</param>
    public Db2Command(string cmdText, DbConnection connection)
    {
        _commandText = cmdText;
        _dbConnection = connection;
    }

    /// <summary>
    /// Gets or sets the SQL statement or stored procedure to execute at the data source.
    /// </summary>
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value;
    }

    /// <summary>
    /// Gets or sets the wait time (in seconds) before terminating the attempt to execute a command and generating an error.
    /// </summary>
    public override int CommandTimeout
    {
        get => _commandTimeout;
        set => _commandTimeout = value;
    }

    /// <summary>
    /// Gets or sets a value indicating how the <see cref="CommandText"/> property is to be interpreted.
    /// Supports <see cref="CommandType.Text"/> or <see cref="CommandType.StoredProcedure"/>.
    /// </summary>
    public override CommandType CommandType
    {
        get => _commandType;
        set => _commandType = value;
    }

    /// <summary>
    /// Gets or sets a value indicating whether the command object should be visible in a customized Windows Forms Designer control.
    /// </summary>
    public override bool DesignTimeVisible
    {
        get => _designTimeVisible;
        set => _designTimeVisible = value;
    }

    /// <summary>
    /// Gets or sets how command results are applied to the DataRow when used by the DbDataAdapter.Update method.
    /// </summary>
    public override UpdateRowSource UpdatedRowSource
    {
        get => _updateRowSource;
        set => _updateRowSource = value;
    }

    /// <summary>
    /// Gets or sets the <see cref="DbConnection"/> used by this <see cref="DbCommand"/>.
    /// </summary>
    protected override DbConnection? DbConnection
    {
        get => _dbConnection;
        set => _dbConnection = value;
    }

    /// <summary>
    /// Gets the collection of <see cref="DbParameter"/> objects.
    /// Parameter substitution currently implements a client-side rewrite of ? markers into formatted literals.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <summary>
    /// Gets or sets the <see cref="DbTransaction"/> within which this <see cref="DbCommand"/> object executes.
    /// </summary>
    protected override DbTransaction? DbTransaction
    {
        get => _dbTransaction;
        set => _dbTransaction = value;
    }

    /// <summary>
    /// Attempts to cancel the execution of a <see cref="DbCommand"/>.
    /// NOTE: Not fully implemented. Usually requires issuing a socket interrupt command via DRDA.
    /// </summary>
    public override void Cancel()
    {
        // TODO: Issue an interrupt socket command
    }

    /// <summary>
    /// Executes a SQL statement against the connection and returns the number of rows affected.
    /// Typically used for INSERT, UPDATE, or DELETE statements.
    /// </summary>
    /// <returns>The number of rows affected.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the connection is missing, closed, or set to read-only mode.
    /// </exception>
    public override int ExecuteNonQuery()
    {
        if (_dbConnection is not Db2Connection db2conn || db2conn.HostServerManager == null)
            throw new InvalidOperationException("Connection not set or closed");

        // Prevent modification commands if the connection is in read-only mode
        if (db2conn.IsReadOnly)
            throw new InvalidOperationException("Connection is read-only. ExecuteNonQuery is not allowed.");

        string sql = ResolveSql();
        // Dispatches the non-query command to the host server network manager
        return db2conn.HostServerManager.ExecuteSqlAsync(sql).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Executes the query, and returns the first column of the first row in the result set returned by the query.
    /// Additional columns or rows are ignored.
    /// </summary>
    /// <returns>The first column of the first row in the result set, or null if the result set is empty.</returns>
    public override object? ExecuteScalar()
    {
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        if (reader.Read() && reader.FieldCount > 0)
        {
            return reader.GetValue(0);
        }
        return null; // Return null if nothing found
    }

    /// <summary>
    /// Creates a prepared version of the command on the data source.
    /// NOTE: Optimization hook for future implementation using PRPSQLSTT DRDA command.
    /// </summary>
    public override void Prepare()
    {
        // TODO: Issue PRPSQLSTT early to pre-compile the SQL statement on the host server
    }

    /// <summary>
    /// Creates a new instance of a <see cref="DbParameter"/> object.
    /// </summary>
    /// <returns>A new <see cref="Db2Parameter"/>.</returns>
    protected override DbParameter CreateDbParameter()
    {
        return new Db2Parameter();
    }

    /// <summary>
    /// Executes the command text against the connection and returns a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the connection is not set or is closed.</exception>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (_dbConnection is not Db2Connection db2conn || db2conn.HostServerManager == null)
            throw new InvalidOperationException("Connection not set or closed");

        string sql = ResolveSql();
        
        // Execute the query synchronously over the underlying asynchronous host server API
        var res = db2conn.HostServerManager.OpenQueryAsync(sql).GetAwaiter().GetResult();
        
        // Wrap the network response into an ADO.NET DataReader
        return new Db2DataReader(res, db2conn.HostServerManager, db2conn.HostServerManager.LastCursorName);
    }

    /// <summary>
    /// Computes the final SQL string to send to the host server.
    /// Handles <see cref="CommandType.StoredProcedure"/> conversion, checks for parameters, 
    /// and invokes <see cref="SubstituteParameters"/> if parameters are present.
    /// </summary>
    private string ResolveSql()
    {
        string sql = _commandText;

        // If defined as StoredProcedure, wrap the text into standard CALL syntax
        if (_commandType == CommandType.StoredProcedure)
        {
            if (_parameters.Count > 0)
                sql = $"CALL {sql}({string.Join(", ", Enumerable.Range(0, _parameters.Count).Select(_ => "?"))})";
            else
                sql = $"CALL {sql}()";
        }

        // Fast path: if no parameters, return plain text
        if (_parameters.Count == 0)
            return sql;

        // Otherwise inject parameters inline
        return SubstituteParameters(sql);
    }

    /// <summary>
    /// A temporary/naive parsing mechanism which manually escapes parameter values 
    /// and injects them where '?' placeholders exist in the <see cref="CommandText"/>.
    /// NOTE: Future versions should use true parameter binding over the DRDA protocol.
    /// </summary>
    private string SubstituteParameters(string sql)
    {
        var result = new StringBuilder(sql.Length + _parameters.Count * 32);
        int paramIndex = 0;
        bool inString = false;

        // Loop through the input SQL character by character
        for (int i = 0; i < sql.Length; i++)
        {
            char c = sql[i];

            if (c == '\'')
            {
                // Simple state machine to detect escaped quotes within string literals
                if (inString && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    result.Append("''"); // Output escaped quote
                    i++; // Skip the second quote
                    continue;
                }
                
                inString = !inString; // Toggle string state
                result.Append(c);
            }
            else if (c == '?' && !inString)
            {
                // We've found an active substitution marker outside of a string literal
                if (paramIndex < _parameters.Count)
                {
                    // Inject the formatted parameter literal directly into the SQL
                    result.Append(FormatParameterValue(_parameters[paramIndex]));
                    paramIndex++;
                }
                else
                {
                    // More markers than parameters, leave literal '?' in place
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

    /// <summary>
    /// Converts a .NET object value found in a <see cref="DbParameter"/> into 
    /// its Db2 SQL string equivalent, including proper escaping for strings and dates.
    /// </summary>
    private static string FormatParameterValue(DbParameter param)
    {
        if (param.Value == null || param.Value == DBNull.Value)
            return "NULL";

        var val = param.Value;

        // Pattern matching and formatting for base types
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
            return b ? "'1'" : "'0'"; // Db2 generally treats boolean-like fields as single char or int 1/0
            
        // Complex type formatting
        if (val is DateTime dt)
        {
            if (param.DbType == DbType.Date)
                return "'" + dt.ToString("yyyy-MM-dd") + "'";
            if (param.DbType == DbType.Time)
                return "'" + dt.ToString("HH.mm.ss") + "'";
                
            // Timestamp format
            return "'" + dt.ToString("yyyy-MM-dd-HH.mm.ss.ffffff") + "'";
        }
        
        if (val is byte[] bytes)
            return "X'" + Convert.ToHexString(bytes) + "'"; // Hexadecimal literals
            
        if (val is Guid g)
            return "'" + g.ToString() + "'";
            
        if (val is byte by)
            return by.ToString(CultureInfo.InvariantCulture);

        // Fallback for custom or unknown types
        return "'" + val.ToString()!.Replace("'", "''") + "'";
    }
}

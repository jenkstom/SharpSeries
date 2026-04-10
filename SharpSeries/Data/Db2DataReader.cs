using System;
using System.Data.Common;
using SharpSeries.HostServer;
using SharpSeries.Encoding;

namespace SharpSeries.Data;

/// <summary>
/// Provides a way of reading a forward-only stream of rows from an IBM i Db2 database.
/// This class parses the raw network data returned by the DRDA protocol into .NET types.
/// </summary>
public class Db2DataReader : DbDataReader
{
    private bool _isClosed;
    private int _recordsAffected = -1; // -1 indicates a SELECT statement, as required by ADO.NET
    private QueryResult _result;
    private int _currentIndex = -1; // Pointer to the current row in the result set
    
    // Components required to track and close the server-side cursor
    private readonly HostServerConnectionManager? _manager;
    private readonly string? _cursorName;

    /// <summary>
    /// Initializes a new instance of the <see cref="Db2DataReader"/> class using the decoded query results.
    /// </summary>
    /// <param name="result">The structured result object containing column definitions and raw row data.</param>
    /// <param name="manager">The host server connection manager used to communicate with the database.</param>
    /// <param name="cursorName">The name of the cursor on the server. Required to clean up server resources upon closing.</param>
    public Db2DataReader(QueryResult result, HostServerConnectionManager? manager = null, string? cursorName = null)
    {
        _result = result ?? new QueryResult();
        _manager = manager;
        _cursorName = cursorName;
    }

    /// <summary>
    /// Gets a value indicating the depth of nesting for the current row. Always 0 since Db2 doesn't support nested result sets.
    /// </summary>
    public override int Depth => 0;

    /// <summary>
    /// Gets a value indicating whether the data reader is closed.
    /// </summary>
    public override bool IsClosed => _isClosed;

    /// <summary>
    /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
    /// </summary>
    public override int RecordsAffected => _recordsAffected;

    /// <summary>
    /// Gets the number of columns in the current row.
    /// </summary>
    public override int FieldCount => _result.Columns.Count;

    /// <summary>
    /// Gets a value that indicates whether this <see cref="Db2DataReader"/> contains one or more rows.
    /// </summary>
    public override bool HasRows => _result.Rows.Count > 0;

    /// <summary>
    /// Releases the managed resources used by the <see cref="Db2DataReader"/> and optionally releases the unmanaged resources.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        if (!_isClosed) Close();
        base.Dispose(disposing);
    }

    /// <summary>
    /// Closes the <see cref="Db2DataReader"/> object.
    /// This also attempts to close the server-side cursor over the DRDA network link.
    /// </summary>
    public override void Close()
    {
        if (_isClosed) return;
        
        // If we have an active cursor and connection, tell the server we're done
        if (_manager != null && _cursorName != null)
        {
            try
            {
                // Synchronously close the cursor and commit to free up locks/resources
                _manager.CloseCursorAsync(_cursorName).GetAwaiter().GetResult();
                _manager.CommitAsync().GetAwaiter().GetResult();
            }
            catch 
            { 
                // Swallow errors on close to prevent crashing the caller if the network dropped
            }
        }
        _isClosed = true;
    }

    /// <summary>
    /// Advances the <see cref="Db2DataReader"/> to the next record.
    /// </summary>
    /// <returns>True if there are more rows; otherwise false.</returns>
    public override bool Read()
    {
        if (_currentIndex + 1 < _result.Rows.Count)
        {
            _currentIndex++;
            return true;
        }
        return false;
    }

    /// <summary>
    /// Advances the reader to the next result when reading the results of batch SQL statements.
    /// Currently not supported by this provider.
    /// </summary>
    public override bool NextResult() => false;

    /// <summary>
    /// Gets the value of the specified column in its native format.
    /// This method is the core parsing engine that reads raw bytes from the network response 
    /// and converts them into appropriate .NET data types based on the column's DRDA type code.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column as a .NET object.</returns>
    public override object GetValue(int ordinal)
    {
        if (_currentIndex < 0 || _currentIndex >= _result.Rows.Count) 
            throw new InvalidOperationException("No current row.");

        // Check if the database marked this specific field as NULL
        if (_currentIndex < _result.NullIndicators.Count)
        {
            var indicators = _result.NullIndicators[_currentIndex];
            // An indicator < 0 means the field value is DB NULL
            if (ordinal < indicators.Length && indicators[ordinal] < 0)
                return DBNull.Value;
        }

        var rowData = _result.Rows[_currentIndex];
        var col = _result.Columns[ordinal];
        
        // Calculate where our specific column's data starts in the continuous byte array for this row
        int offset = 0;
        for (int i = 0; i < ordinal; i++)
        {
            offset += _result.Columns[i].Length;
        }
        
        if (offset >= rowData.Length) return DBNull.Value;
        
        // Extract the exact bytes pertaining only to this cell
        int readLen = Math.Min(col.Length, rowData.Length - offset);
        byte[] cell = new byte[readLen];
        Array.Copy(rowData, offset, cell, 0, readLen);

        // DRDA type codes: even numbers usually indicate NOT NULL, odd numbers indicate NULLABLE
        // We strip the lowest bit to simplify switching logic
        int t = col.Type & ~1;

        try {
            switch(t) {
                case 452: // CHAR (Fixed Length)
                case 453:
                    // Convert raw EBCDIC bytes directly to a .NET string using the IBM i CCSID
                    return CcsidConverter.GetString(col.Ccsid, cell).TrimEnd();
                    
                case 448: // VARCHAR (Variable Length)
                case 449:
                    // VARCHARs start with a 2-byte length header in big-endian format
                    int strLen = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(cell.AsSpan(0, 2));
                    return CcsidConverter.GetString(col.Ccsid, cell.AsSpan(2, strLen)).TrimEnd();
                    
                case 484: // PACKED DECIMAL (COMP-3)
                case 485:
                    return SharpSeries.Types.DecimalConverter.ReadPackedDecimal(cell, col.Scale);
                    
                case 488: // ZONED DECIMAL
                case 489:
                    return SharpSeries.Types.DecimalConverter.ReadZonedDecimal(cell, col.Scale);
                    
                case 496: // INTEGER (4-byte)
                case 497:
                    // IBM i always transmits numerics in big-endian format
                    return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(cell);
                    
                case 500: // SMALLINT (2-byte)
                case 501:
                    return System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(cell);
                    
                case 492: // BIGINT (8-byte)
                case 493:
                    return System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(cell);
                    
                case 384: // DATE
                case 385:
                    return SharpSeries.Types.DateTimeConverter.ReadDate(cell, col.Ccsid);
                    
                case 392: // TIMESTAMP
                case 393:
                    return SharpSeries.Types.DateTimeConverter.ReadTimestamp(cell, col.Ccsid);
                    
                default:
                    // Fallback to reading as standard EBCDIC text if unknown
                    return CcsidConverter.GetString(37, cell).TrimEnd();
            }
        } catch {
            return "???"; // Safe failure if parsing logic bombs out
        }
    }

    /// <summary>
    /// Gets a value that indicates whether the column contains non-existent or missing values.
    /// </summary>
    public override bool IsDBNull(int ordinal)
    {
        if (_currentIndex < 0 || _currentIndex >= _result.NullIndicators.Count)
            return false;
            
        var indicators = _result.NullIndicators[_currentIndex];
        return ordinal < indicators.Length && indicators[ordinal] < 0;
    }

    /// <summary>
    /// Gets the name of the specified column.
    /// </summary>
    public override string GetName(int ordinal) => _result.Columns[ordinal].Name;

    /// <summary>
    /// Gets the column ordinal, given the name of the column.
    /// </summary>
    public override int GetOrdinal(string name) => _result.Columns.FindIndex(c => c.Name == name);

    /// <summary>
    /// Gets a string representing the data type of the specified column.
    /// </summary>
    public override string GetDataTypeName(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _result.Columns.Count) throw new ArgumentOutOfRangeException(nameof(ordinal));
        
        int t = _result.Columns[ordinal].Type & ~1;
        return t switch
        {
            452 or 453 => "CHAR",
            448 or 449 => "VARCHAR",
            484 or 485 => "DECIMAL",
            488 or 489 => "DECIMAL",
            496 or 497 => "INTEGER",
            500 or 501 => "SMALLINT",
            492 or 493 => "BIGINT",
            384 or 385 => "DATE",
            392 or 393 => "TIMESTAMP",
            _ => "VARCHAR" // Default fallback string representation
        };
    }

    /// <summary>
    /// Gets the .NET <see cref="Type"/> that is the data type of the object.
    /// </summary>
    public override Type GetFieldType(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _result.Columns.Count) throw new ArgumentOutOfRangeException(nameof(ordinal));
        
        int t = _result.Columns[ordinal].Type & ~1;
        return t switch
        {
            452 or 453 or 448 or 449 => typeof(string),
            484 or 485 or 488 or 489 => typeof(decimal),
            496 or 497 => typeof(int),
            500 or 501 => typeof(short),
            492 or 493 => typeof(long),
            384 or 385 or 392 or 393 => typeof(DateTime),
            _ => typeof(string)
        };
    }

    // Standard ADO.NET strongly-typed accessors.
    // These retrieve the value as an object using GetValue() and forcibly cast to expected types.
    
    public override string GetString(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return val.ToString()!;
    }

    public override int GetInt32(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToInt32(val);
    }

    public override short GetInt16(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToInt16(val);
    }

    public override long GetInt64(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToInt64(val);
    }

    public override byte GetByte(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToByte(val);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0; // Unimplemented
    
    public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));
    
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0; // Unimplemented

    public override decimal GetDecimal(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToDecimal(val);
    }

    public override double GetDouble(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToDouble(val);
    }

    public override float GetFloat(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToSingle(val);
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        return Convert.ToDateTime(val);
    }

    public override Guid GetGuid(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val is Guid g) return g;
        throw new InvalidCastException("Column is not a Guid");
    }

    public override bool GetBoolean(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value) throw new InvalidCastException("Column is null");
        // Db2 generally operates heavily with 1/0 or Y/N rather than true boolean bits
        if (val is string s)
            return s == "1" || s.Equals("Y", StringComparison.OrdinalIgnoreCase) || s.Equals("true", StringComparison.OrdinalIgnoreCase);
        return Convert.ToBoolean(val);
    }

    public override System.Collections.IEnumerator GetEnumerator() => throw new NotImplementedException();

    /// <summary>
    /// Populates an array of objects with the column values of the current record.
    /// </summary>
    public override int GetValues(object[] values)
    {
        int count = Math.Min(FieldCount, values.Length);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    /// <summary>
    /// Gets the value of the specified column in its native format given the column ordinal.
    /// </summary>
    public override object this[int ordinal] => GetValue(ordinal);
    
    /// <summary>
    /// Gets the value of the specified column in its native format given the column name.
    /// </summary>
    public override object this[string name] => GetValue(GetOrdinal(name));
}

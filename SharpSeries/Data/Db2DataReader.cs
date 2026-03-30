using System;
using System.Data.Common;
using SharpSeries.HostServer;
using SharpSeries.Encoding;

namespace SharpSeries.Data;

public class Db2DataReader : DbDataReader
{
    private bool _isClosed;
    private int _recordsAffected = -1; // -1 for SELECTs
    private QueryResult _result;
    private int _currentIndex = -1;
    private readonly HostServerConnectionManager? _manager;
    private readonly string? _cursorName;

    public Db2DataReader(QueryResult result, HostServerConnectionManager? manager = null, string? cursorName = null)
    {
        _result = result ?? new QueryResult();
        _manager = manager;
        _cursorName = cursorName;
    }

    public override int Depth => 0;
    public override bool IsClosed => _isClosed;
    public override int RecordsAffected => _recordsAffected;
    public override int FieldCount => _result.Columns.Count;
    public override bool HasRows => _result.Rows.Count > 0;

    protected override void Dispose(bool disposing)
    {
        if (!_isClosed) Close();
        base.Dispose(disposing);
    }

    public override void Close()
    {
        if (_isClosed) return;
        if (_manager != null && _cursorName != null)
        {
            try
            {
                _manager.CloseCursorAsync(_cursorName).GetAwaiter().GetResult();
                _manager.CommitAsync().GetAwaiter().GetResult();
            }
            catch { }
        }
        _isClosed = true;
    }

    public override bool Read()
    {
        if (_currentIndex + 1 < _result.Rows.Count)
        {
            _currentIndex++;
            return true;
        }
        return false;
    }

    public override bool NextResult() => false;

    public override object GetValue(int ordinal)
    {
        if (_currentIndex < 0 || _currentIndex >= _result.Rows.Count) throw new InvalidOperationException("No current row.");

        if (_currentIndex < _result.NullIndicators.Count)
        {
            var indicators = _result.NullIndicators[_currentIndex];
            if (ordinal < indicators.Length && indicators[ordinal] < 0)
                return DBNull.Value;
        }

        var rowData = _result.Rows[_currentIndex];
        var col = _result.Columns[ordinal];
        
        int offset = 0;
        for (int i = 0; i < ordinal; i++)
        {
            offset += _result.Columns[i].Length;
        }
        
        if (offset >= rowData.Length) return DBNull.Value;
        
        int readLen = Math.Min(col.Length, rowData.Length - offset);
        byte[] cell = new byte[readLen];
        Array.Copy(rowData, offset, cell, 0, readLen);

        int t = col.Type & ~1;

        try {
            switch(t) {
                case 452: // CHAR
                case 453:
                    return CcsidConverter.GetString(col.Ccsid, cell).TrimEnd();
                case 448: // VARCHAR
                case 449:
                    int strLen = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(cell.AsSpan(0, 2));
                    return CcsidConverter.GetString(col.Ccsid, cell.AsSpan(2, strLen)).TrimEnd();
                case 484: // PACKED DECIMAL
                case 485:
                    return SharpSeries.Types.DecimalConverter.ReadPackedDecimal(cell, col.Scale);
                case 488: // ZONED DECIMAL
                case 489:
                    return SharpSeries.Types.DecimalConverter.ReadZonedDecimal(cell, col.Scale);
                case 496: // INTEGER
                case 497:
                    return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(cell);
                case 500: // SMALLINT
                case 501:
                    return System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(cell);
                case 492: // BIGINT
                case 493:
                    return System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(cell);
                case 384: // DATE
                case 385:
                    return SharpSeries.Types.DateTimeConverter.ReadDate(cell, col.Ccsid);
                case 392: // TIMESTAMP
                case 393:
                    return SharpSeries.Types.DateTimeConverter.ReadTimestamp(cell, col.Ccsid);
                default:
                    return CcsidConverter.GetString(37, cell).TrimEnd();
            }
        } catch {
            return "???";
        }
    }

    public override bool IsDBNull(int ordinal)
    {
        if (_currentIndex < 0 || _currentIndex >= _result.NullIndicators.Count)
            return false;
        var indicators = _result.NullIndicators[_currentIndex];
        return ordinal < indicators.Length && indicators[ordinal] < 0;
    }

    public override string GetName(int ordinal) => _result.Columns[ordinal].Name;
    public override int GetOrdinal(string name) => _result.Columns.FindIndex(c => c.Name == name);

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
            _ => "VARCHAR"
        };
    }

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

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
    public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;

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
        if (val is string s)
            return s == "1" || s.Equals("Y", StringComparison.OrdinalIgnoreCase) || s.Equals("true", StringComparison.OrdinalIgnoreCase);
        return Convert.ToBoolean(val);
    }

    public override System.Collections.IEnumerator GetEnumerator() => throw new NotImplementedException();

    public override int GetValues(object[] values)
    {
        int count = Math.Min(FieldCount, values.Length);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
}

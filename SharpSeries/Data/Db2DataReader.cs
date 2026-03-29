using System;
using System.Data.Common;
using System.Text;
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
        if (_currentIndex < 0 || _currentIndex >= _result.Rows.Count) throw new InvalidOperationException();
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

        int t = col.Type;
        bool isNullable = (t % 2 != 0); // ODD means nullable, but we didn't parse indicators, so cross fingers it's not null or ignore for now.
        t = t & ~1; // make it even

        try {
            switch(t) {
                case 452: // CHAR
                case 453:
                    return SharpSeries.Encoding.CcsidConverter.GetString(col.Ccsid, cell).TrimEnd();
                case 448: // VARCHAR
                case 449:
                    // 2 byte length
                    int strLen = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(cell.AsSpan(0, 2));
                    return SharpSeries.Encoding.CcsidConverter.GetString(col.Ccsid, cell.AsSpan(2, strLen)).TrimEnd();
                case 484: // PACKED
                case 485:
                    return SharpSeries.Types.DecimalConverter.ReadPackedDecimal(cell, col.Scale);
                case 488: // ZONED
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
                    // Fallback string conversion for EBCDIC char types
                    return SharpSeries.Encoding.CcsidConverter.GetString(37, cell).TrimEnd();
            }
        } catch {
            return "???";
        }
    }


    public override bool IsDBNull(int ordinal) => false;
    public override string GetName(int ordinal) => _result.Columns[ordinal].Name;
    public override int GetOrdinal(string name) => _result.Columns.FindIndex(c => c.Name == name);
    public override string GetDataTypeName(int ordinal) => "VARCHAR";
    public override Type GetFieldType(int ordinal) => typeof(string);

    public override string GetString(int ordinal) => GetValue(ordinal).ToString() ?? "";
    public override int GetInt32(int ordinal) => 0;
    public override short GetInt16(int ordinal) => 0;
    public override long GetInt64(int ordinal) => 0;
    public override byte GetByte(int ordinal) => 0;
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
    public override char GetChar(int ordinal) => ' ';
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
    public override decimal GetDecimal(int ordinal) => 0m;
    public override double GetDouble(int ordinal) => 0d;
    public override float GetFloat(int ordinal) => 0f;
    public override DateTime GetDateTime(int ordinal) => DateTime.Now;
    public override Guid GetGuid(int ordinal) => Guid.Empty;
    public override bool GetBoolean(int ordinal) => false;

    public override System.Collections.IEnumerator GetEnumerator() => throw new NotImplementedException();
    public override int GetValues(object[] values) => 0;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
}

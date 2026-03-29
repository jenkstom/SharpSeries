#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
#pragma warning disable CS0414 // Field is assigned but its value is never used
#pragma warning disable CS0169 // Field is never used
using System.Data;
using System.Data.Common;

namespace SharpSeries.Data;

public sealed class Db2Parameter : DbParameter
{
    private string _name = string.Empty;
    private object? _value;
    private DbType _dbType = DbType.String;
    private ParameterDirection _direction = ParameterDirection.Input;
    private bool _isNullable = true;
    private string _sourceColumn = string.Empty;
    private DataRowVersion _sourceVersion = DataRowVersion.Default;
    private int _size;
    private byte _precision;
    private byte _scale;

    public override DbType DbType { get => _dbType; set => _dbType = value; }
    public override ParameterDirection Direction { get => _direction; set => _direction = value; }
    public override bool IsNullable { get => _isNullable; set => _isNullable = value; }
    public override string ParameterName { get => _name; set => _name = value; }
    public override int Size { get => _size; set => _size = value; }
    public override string SourceColumn { get => _sourceColumn; set => _sourceColumn = value; }
    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get => _value; set => _value = value; }

    public override void ResetDbType()
    {
        _dbType = DbType.String;
    }
}

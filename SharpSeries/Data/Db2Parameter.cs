#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member
#pragma warning disable CS0414 // Field is assigned but its value is never used
#pragma warning disable CS0169 // Field is never used
using System.Data;
using System.Data.Common;

namespace SharpSeries.Data;

/// <summary>
/// Represents a user-provided parameter to a <see cref="Db2Command"/>.
/// Serves as a type-aware holding vessel for data before it is formatted into an SQL literal string natively within the command parser.
/// </summary>
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

    /// <summary>
    /// Gets or sets the <see cref="DbType"/> of the parameter to help with type-formatting.
    /// The default is <see cref="DbType.String"/>.
    /// </summary>
    public override DbType DbType { get => _dbType; set => _dbType = value; }

    /// <summary>
    /// Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional, or a return value.
    /// Note: Output parameters via DRDA stored procedures are not supported in the current implementation.
    /// </summary>
    public override ParameterDirection Direction { get => _direction; set => _direction = value; }

    /// <summary>
    /// Gets or sets a value indicating whether the parameter accepts null values.
    /// </summary>
    public override bool IsNullable { get => _isNullable; set => _isNullable = value; }

    /// <summary>
    /// Gets or sets the name of the DbParameter.
    /// Note: Named parameters (e.g. @id) are unsupported; parameters must be referenced natively via positional question marks (?).
    /// </summary>
    public override string ParameterName { get => _name; set => _name = value; }

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of the data within the column.
    /// </summary>
    public override int Size { get => _size; set => _size = value; }

    /// <summary>
    /// Gets or sets the name of the source column that is mapped to the <see cref="DataSet"/> and used for loading or returning the <see cref="Value"/>.
    /// </summary>
    public override string SourceColumn { get => _sourceColumn; set => _sourceColumn = value; }

    /// <summary>
    /// Sets or gets a value which indicates whether the source column is nullable.
    /// </summary>
    public override bool SourceColumnNullMapping { get; set; }

    /// <summary>
    /// Gets or sets the value of the parameter dynamically at runtime.
    /// Can be nearly any primitive .NET DataType, null, or DBNull.
    /// </summary>
    public override object? Value { get => _value; set => _value = value; }

    /// <summary>
    /// Resets the <see cref="DbType"/> property to its original state (String).
    /// </summary>
    public override void ResetDbType()
    {
        _dbType = DbType.String;
    }
}

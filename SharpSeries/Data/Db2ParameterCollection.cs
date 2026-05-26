using System.Collections;
using System.Data.Common;

namespace SharpSeries.Data;

/// <summary>
/// A collection of <see cref="Db2Parameter"/> objects representing the parameters 
/// bound to a <see cref="Db2Command"/>.
/// Supports sequential addition, lookup by index, or lookup by parameter name.
/// </summary>
public sealed class Db2ParameterCollection : DbParameterCollection
{
    private List<DbParameter> _parameters = new();

    /// <summary>
    /// Gets the number of <see cref="DbParameter"/> objects in the collection.
    /// </summary>
    public override int Count => _parameters.Count;

    /// <summary>
    /// Returns a value that specifies whether the collection has a fixed size.
    /// Always false for <see cref="Db2ParameterCollection"/>.
    /// </summary>
    public override bool IsFixedSize => false;

    /// <summary>
    /// Returns a value indicating whether the collection is read-only.
    /// Always false.
    /// </summary>
    public override bool IsReadOnly => false;

    /// <summary>
    /// Returns a value indicating whether access to the collection is synchronized.
    /// Always false.
    /// </summary>
    public override bool IsSynchronized => false;

    /// <summary>
    /// Returns an object that can be used to synchronize access to the collection.
    /// </summary>
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    /// <summary>
    /// Adds the specified <see cref="DbParameter"/> to the collection.
    /// </summary>
    /// <param name="value">The parameter to add. Must be derived from DbParameter.</param>
    /// <returns>The index of the newly added parameter.</returns>
    public override int Add(object value)
    {
        _parameters.Add((DbParameter)value);
        return Count - 1;
    }

    /// <summary>
    /// Adds an array of values to the collection in order.
    /// </summary>
    public override void AddRange(Array values)
    {
        foreach (var value in values)
        {
            Add(value);
        }
    }

    /// <summary>
    /// Clears the entire collection.
    /// </summary>
    public override void Clear() => _parameters.Clear();

    /// <summary>
    /// Checks if a given parameter exists within the collection by reference.
    /// </summary>
    public override bool Contains(object value) => _parameters.Contains((DbParameter)value);

    /// <summary>
    /// Checks if a parameter with the given parameter name exists in the collection. Case-insensitive.
    /// </summary>
    public override bool Contains(string value) => IndexOf(value) != -1;

    /// <summary>
    /// Copies all the elements of the current collection to the specified Array, starting at the specified destination index.
    /// </summary>
    public override void CopyTo(Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);

    /// <summary>
    /// Returns an enumerator that iterates through the parameter collection.
    /// </summary>
    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    /// <summary>
    /// Returns the exact index of the specified parameter in the collection.
    /// </summary>
    public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);

    /// <summary>
    /// Returns the index of a parameter matching the specified name.
    /// Note: Named parameters are poorly supported during final SQL generation; parameters are processed sequentially.
    /// </summary>
    public override int IndexOf(string parameterName)
    {
        return _parameters.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Inserts a parameter into the collection at the specified index.
    /// </summary>
    public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);

    /// <summary>
    /// Removes the specified parameter object from the collection.
    /// </summary>
    public override void Remove(object value) => _parameters.Remove((DbParameter)value);

    /// <summary>
    /// Removes a parameter at the specified physical index from the collection.
    /// </summary>
    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    /// <summary>
    /// Removes a parameter from the collection matching the specified parameter name.
    /// </summary>
    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index != -1)
            RemoveAt(index);
    }

    /// <summary>
    /// Retrieves a parameter by its numerical offset (index) from the collection.
    /// Used by ADO.NET internals when indexing an array.
    /// </summary>
    protected override DbParameter GetParameter(int index) => _parameters[index];

    /// <summary>
    /// Retrieves a parameter by its string name from the collection.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the parameter name does not exist.</exception>
    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter {parameterName} not found.");
        return _parameters[index];
    }

    /// <summary>
    /// Updates the internal array value at the specified index with a new <see cref="DbParameter"/>.
    /// </summary>
    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;

    /// <summary>
    /// Overwrites an existing parameter matching the specified name with a new <see cref="DbParameter"/>.
    /// If no match exists, simply adds the parameter to the end of the collection.
    /// </summary>
    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            _parameters.Add(value);
        else
            _parameters[index] = value;
    }
}

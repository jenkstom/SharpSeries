using System.Collections;
using System.Data.Common;

namespace SharpSeries.Data;

public sealed class Db2ParameterCollection : DbParameterCollection
{
    private List<DbParameter> _parameters = new();

    public override int Count => _parameters.Count;
    public override bool IsFixedSize => false;
    public override bool IsReadOnly => false;
    public override bool IsSynchronized => false;
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    public override int Add(object value)
    {
        _parameters.Add((DbParameter)value);
        return Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var value in values)
        {
            Add(value);
        }
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => _parameters.Contains((DbParameter)value);

    public override bool Contains(string value) => IndexOf(value) != -1;

    public override void CopyTo(Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);

    public override int IndexOf(string parameterName)
    {
        return _parameters.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
    }

    public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);

    public override void Remove(object value) => _parameters.Remove((DbParameter)value);

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index != -1)
            RemoveAt(index);
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter {parameterName} not found.");
        return _parameters[index];
    }

    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            _parameters.Add(value);
        else
            _parameters[index] = value;
    }
}

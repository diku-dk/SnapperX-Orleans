using System.Diagnostics;

namespace Concurrency.Common.State;

[GenerateSerializer]
public class SnapperRecord : IEquatable<SnapperRecord>, ICloneable
{
    [Id(0)]
    public readonly ISnapperValue record;

    [Id(1)]
    public readonly string recordTypeName;

    public SnapperRecord(ISnapperValue record)
    {
        this.record = record;
        var name = record.GetAssemblyQualifiedName();
        Debug.Assert(!string.IsNullOrEmpty(name));
        recordTypeName = name;
    }

    public bool Equals(SnapperRecord? other) => other != null && other.record.Equals(record) && other.recordTypeName == recordTypeName;

    public object Clone()
    {
        var recordCopy = record.Clone() as ISnapperValue;
        Debug.Assert(recordCopy != null);
        return new SnapperRecord(recordCopy);
    }
}
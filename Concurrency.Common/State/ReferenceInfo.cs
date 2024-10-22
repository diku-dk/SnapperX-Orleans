using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.State;

[GenerateSerializer]
public class ReferenceInfo : ICloneable, IEquatable<ReferenceInfo>
{
    [Id(0)]
    public readonly SnapperKeyReferenceType referenceType;

    [Id(1)]
    public readonly GrainID grain1;         // origin grain

    [Id(2)]
    public readonly ISnapperKey key1;       // origin key

    [Id(3)]
    public readonly GrainID grain2;         // follower grian

    [Id(4)]
    public readonly ISnapperKey key2;       // follower key

    /// <summary> the function to call when forward update to key2 </summary>
    [Id(5)]
    public readonly IUpdateFunction function;

    public ReferenceInfo(SnapperKeyReferenceType referenceType, GrainID grain1, ISnapperKey key1, GrainID grain2, ISnapperKey key2, IUpdateFunction function)
    {
        var name = function.GetAssemblyQualifiedName();
        Debug.Assert(name != null);

        switch (referenceType)
        {
            case SnapperKeyReferenceType.DeleteReference:
                var name1 = new DefaultFunction().GetAssemblyQualifiedName();
                Debug.Assert(name.Equals(name1));
                break;
            case SnapperKeyReferenceType.ReplicateReference:
                var name2 = new DefaultUpdateFunction().GetAssemblyQualifiedName();
                Debug.Assert(name.Equals(name2));
                break;
        }

        this.referenceType = referenceType;
        this.grain1 = grain1;
        this.key1 = key1;
        this.grain2 = grain2;
        this.key2 = key2;
        this.function = function;
    }

    public object Clone() => new ReferenceInfo(referenceType, (GrainID)grain1.Clone(), (ISnapperKey)key1.Clone(), (GrainID)grain2.Clone(), (ISnapperKey)key2.Clone(), (IUpdateFunction)function.Clone());

    public bool Equals(ReferenceInfo? other)
    {
        if (other == null) return false;

        return other.referenceType.Equals(referenceType) &&
            other.grain1.Equals(grain1) && other.key1.Equals(key1) &&
            other.grain2.Equals(grain2) && other.key2.Equals(key2) &&
            other.function.Equals(function);
    }
}
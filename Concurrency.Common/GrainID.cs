using MessagePack;
using Utilities;

namespace Concurrency.Common;

[MessagePackObject]
[GenerateSerializer]
public class GrainID : IEquatable<GrainID>, IComparable<GrainID>, ICloneable
{
    [Key(0)]
    [Id(0)]
    public readonly Guid id;

    /// <summary> only transactional actors have className set </summary>
    [Key(1)]
    [Id(1)]
    public readonly string className;

    public GrainID(Guid id, string className)
    {
        this.id = id;
        this.className = className;
    }

    public object Clone() => new GrainID(id, (string)className.Clone());

    public int CompareTo(GrainID? other)
    {
        if (other == null) throw new Exception($"It is not possible to compare a GrainID to null. ");
        if (className != other.className) return className.CompareTo(other.className);
        else return id.CompareTo(other.id);
    }

    public bool Equals(GrainID? other) => other != null && id == other.id && className == other.className;

    public override int GetHashCode() => id.GetHashCode() ^ className.GetHashCode();
    
    public string Print()
    {
        var idStr = Helper.ConvertGuidToIntString(id);
        if (string.IsNullOrEmpty(className)) return idStr;
        else
        {
            if (className.Contains("AccountGrain")) return idStr;

            var strs = className.Split(".");
            return $"{strs[strs.Length - 1]}-{idStr}";
        }
    }
}
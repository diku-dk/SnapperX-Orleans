using MessagePack;

namespace Concurrency.Common;

[MessagePackObject]
[GenerateSerializer]
public class SnapperGrainID : IEquatable<SnapperGrainID>, IComparable<SnapperGrainID>
{
    [Id(0)]
    [Key(0)]
    public readonly GrainID grainID;

    /// <summary> location = region ID "+" silo ID </summary>
    [Id(1)]
    [Key(1)]
    public readonly string location;

    public SnapperGrainID(GrainID grainID, string location) { this.grainID = grainID; this.location = location; }

    public SnapperGrainID(Guid id, string location) { grainID = new GrainID(id, ""); this.location = location; }

    public SnapperGrainID(Guid id, string location, string className) { grainID = new GrainID(id, className); this.location = location; }

    public bool Equals(SnapperGrainID? other) => other != null && location == other.location && grainID.Equals(other.grainID);

    public override int GetHashCode() => location.GetHashCode() ^ grainID.GetHashCode();

    public override string ToString() => grainID.id.ToString() + "+" + grainID.className + "+" + location;

    /// <summary> region ID, silo ID </summary>
    public (string, string) GetLocation()
    {
        var strs = location.Split('+');
        return (strs[0], strs[1]);
    }

    public int CompareTo(SnapperGrainID? other)
    {
        if (other == null) throw new Exception($"It is not possible to compare a SnapperGrainID to null. ");
        if (!grainID.Equals(other.grainID)) return grainID.CompareTo(other.grainID);
        else return location.CompareTo(other.location);
    }

    public bool isEmpty() => string.IsNullOrEmpty(location);
}
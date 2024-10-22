using MessagePack;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common;

[MessagePackObject]
[GenerateSerializer]
public class SnapperID : IEquatable<SnapperID>, IComparable<SnapperID>, ICloneable
{
    [Key(0)]
    [Id(0)]
    public long id;

    [Key(1)]
    [Id(1)]
    public string siloID;

    [Key(2)]
    [Id(2)]
    public string regionID;

    [Key(3)]
    [Id(3)]
    public Hierarchy hierarchy;

    public SnapperID()
    {
        id = -1;
        siloID = string.Empty;
        regionID = string.Empty;
        hierarchy = Hierarchy.Local;
    }

    public SnapperID(long id, string siloID, string regionID, Hierarchy hierarchy)
    {
        this.id = id;
        this.siloID = siloID;
        this.regionID = regionID;
        this.hierarchy = hierarchy;
    }

    public bool Equals(SnapperID? other)
    {
        if (other == null) return false;
        if (id == -1 && other.id == -1) return true;
        return id == other.id && siloID == other.siloID && regionID == other.regionID && hierarchy == other.hierarchy;
    }

    public override int GetHashCode()
    {
        if (id == -1) return id.GetHashCode();
        else return id.GetHashCode() ^ siloID.GetHashCode() ^ regionID.GetHashCode() ^ hierarchy.GetHashCode();
    }

    public bool isEmpty() => id == -1;

    public object Clone() => new SnapperID(id, siloID, regionID, hierarchy);

    public int CompareTo(SnapperID? other)
    {
        if (other == null) throw new Exception($"It is not allowed to compare a SnapperID with null");

        if (isEmpty() && other.isEmpty()) return 0;
        else if (other.isEmpty()) return 1;
        else if (isEmpty()) return -1;
        else if (regionID != other.regionID || siloID != other.siloID || hierarchy != other.hierarchy) throw new Exception($"The two SnapperIDs are not comparable");
        else return id.CompareTo(other.id);
    }

    public string Print()
    {
        if (isEmpty())
        {
            Debug.Assert(string.IsNullOrEmpty(siloID));
            return "";
        }

        var strs = siloID.Split(":");
        var silo = strs[1];
        return id + $" ({silo})";
    }
}
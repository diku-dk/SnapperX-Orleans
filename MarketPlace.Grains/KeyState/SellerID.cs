using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.KeyState;

[MessagePackObject]
[GenerateSerializer]
public class SellerID : AbstractKey<SellerID>, ISnapperKey, IEquatable<SellerID>
{
    [Id(0)]
    [Key(0)]
    public readonly int id;   // this ID is unique among all sellers

    [Id(1)]
    [Key(1)]
    public readonly int baseCityID;

    public SellerID() { id = -1; baseCityID = -1; }

    public SellerID(int id, int baseCityID) { this.id = id; this.baseCityID = baseCityID; }

    public object Clone() => new SellerID(id, baseCityID);

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as SellerID;
            if (data == null) return false;
            return id == data.id && baseCityID == data.baseCityID;
        }
        catch { return false; }
    }

    public bool Equals(SellerID? other)
    {
        if (other == null) return false;
        return id == other.id && baseCityID == other.baseCityID;
    }

    public override int GetHashCode() => id.GetHashCode() ^ baseCityID.GetHashCode();

    public string Print() => $"SellerID: id = {id}, baseCityID = {baseCityID}";
}
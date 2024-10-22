using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.KeyState;

[GenerateSerializer]
[MessagePackObject]
public class PackageID : AbstractKey<PackageID>, ISnapperKey, IEquatable<PackageID>
{
    [Key(0)]
    [Id(0)]
    public readonly SellerID sellerID;

    [Key(1)]
    [Id(1)]
    public readonly int deliveryCityID;

    [Key(2)]
    [Id(2)]
    public readonly long id;    // this id is unique among all packages of this specific seller and in this specific city

    public PackageID() { sellerID = new SellerID(); deliveryCityID = -1; id = -1; }

    public PackageID(SellerID sellerID, int deliveryCityID, long id) { this.sellerID = sellerID; this.deliveryCityID = deliveryCityID; this.id = id; }

    public object Clone() => new PackageID((SellerID)sellerID.Clone(), deliveryCityID, id);

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as PackageID;
            if (data == null) return false;
            return sellerID.Equals(data.sellerID) && deliveryCityID == data.deliveryCityID && id == data.id;
        }
        catch { return false; }
    }

    public bool Equals(PackageID? other)
    {
        if (other == null) return false;
        return sellerID.Equals(other.sellerID) && deliveryCityID == other.deliveryCityID && id == other.id;
    }

    public override int GetHashCode() => sellerID.id.GetHashCode() ^ sellerID.baseCityID.GetHashCode() ^ deliveryCityID.GetHashCode() ^ id.GetHashCode();

    public string Print() => $"PackageID: {sellerID.Print()}, deliveryCityID = {deliveryCityID}, id = {id}";
}
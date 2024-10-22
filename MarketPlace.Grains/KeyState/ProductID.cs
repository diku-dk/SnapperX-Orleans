using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.KeyState;

[MessagePackObject]
[GenerateSerializer]
public class ProductID : AbstractKey<ProductID>, ISnapperKey, IEquatable<ProductID>
{
    [Id(0)]
    [Key(0)]
    public readonly SellerID sellerID;

    [Id(1)]
    [Key(1)]
    public readonly long id;    // this id is unique among all products of this seller

    public ProductID() { sellerID = new SellerID(); id = -1; }

    public ProductID(SellerID sellerID, long id) { this.sellerID = sellerID; this.id = id; }

    public object Clone() => new ProductID((SellerID)sellerID.Clone(), id);

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as ProductID;
            if (data == null) return false;
            return sellerID.Equals(data.sellerID) && id == data.id;
        }
        catch { return false; }
    }

    public bool Equals(ProductID? other)
    {
        if (other == null) return false;
        return sellerID.Equals(other.sellerID) && id == other.id;
    }

    public override int GetHashCode() => sellerID.id.GetHashCode() ^ sellerID.baseCityID.GetHashCode() ^ id.GetHashCode();

    public string Print() => $"ProductID: {sellerID.Print()}, id = {id}";
}
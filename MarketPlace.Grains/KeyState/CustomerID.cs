using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.KeyState;

[GenerateSerializer]
[MessagePackObject]
public class CustomerID : AbstractKey<CustomerID>, ISnapperKey, IEquatable<CustomerID>
{
    [Key(0)]
    [Id(0)]
    public readonly int id;   // this id is unique among all customers

    [Key(1)]
    [Id(1)]
    public readonly int baseCityID;

    public CustomerID() { id = -1; baseCityID = -1; }

    public CustomerID(int id, int baseCityID) { this.id = id; this.baseCityID = baseCityID; }

    public object Clone() => new CustomerID(id, baseCityID);

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as CustomerID;
            if (data == null) return false;
            return id == data.id && baseCityID == data.baseCityID;
        }
        catch { return false; }
    }

    public bool Equals(CustomerID? other)
    {
        if (other == null) return false;
        return id == other.id && baseCityID == other.baseCityID;
    }

    public override int GetHashCode() => id.GetHashCode() ^ baseCityID.GetHashCode();

    public string Print() => $"CustomerID: id = {id}, baseCityID = {baseCityID}";
}

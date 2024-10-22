using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.KeyState;

[GenerateSerializer]
[MessagePackObject]
public class OrderID : AbstractKey<OrderID>, ISnapperKey, IEquatable<OrderID>
{
    [Key(0)]
    [Id(0)]
    public readonly CustomerID customerID;

    [Key(1)]
    [Id(1)]
    public readonly long id;    // a customer may place multiple orders, and they monotonically increasing IDs

    public OrderID() { customerID = new CustomerID(); id = -1; }

    public OrderID(CustomerID customerID, long id) { this.customerID = customerID; this.id = id; }

    public object Clone() => new OrderID((CustomerID)customerID.Clone(), id);

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = (OrderID)other;
            if (data == null) return false;
            return customerID.Equals(data.customerID) && id == data.id;
        }
        catch { return false; }
    }

    public bool Equals(OrderID? other)
    {
        if (other == null) return false;
        return customerID.Equals(other.customerID) && id == other.id;
    }

    public override int GetHashCode() => customerID.id.GetHashCode() ^ customerID.baseCityID.GetHashCode() ^ id.GetHashCode();

    public string Print() => $"OrderID: {customerID.Print()}, id = {id}";
}
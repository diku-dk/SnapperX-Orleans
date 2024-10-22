using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class PackageInfo : AbstractValue<PackageInfo>, ISnapperValue, IEquatable<PackageInfo>
{
    [Key(0)]
    [Id(0)]
    public readonly OrderID orderID;

    [Key(1)]
    [Id(1)]
    public readonly ShipmentInfo shipmentInfo;

    [Key(2)]
    [Id(2)]
    public readonly List<ProductID> items;

    public PackageInfo() 
    { 
        orderID = new OrderID();  
        shipmentInfo = new ShipmentInfo(); 
        items = new List<ProductID>(); 
    }

    public PackageInfo(OrderID orderID, ShipmentInfo shipmentInfo, List<ProductID> items)
    {
        this.orderID = orderID;
        this.shipmentInfo = shipmentInfo;
        this.items = items;
    }

    public object Clone() => new PackageInfo((OrderID)orderID.Clone(), (ShipmentInfo)shipmentInfo.Clone(), new List<ProductID>(items));

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as PackageInfo;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(PackageInfo? data)
    {
        if (data == null) return false;
        if (!orderID.Equals(data.orderID)) return false;
        if (!shipmentInfo.Equals(data.shipmentInfo)) return false;
        if (items.Count != data.items.Count) return false;
        for (var i = 0; i < items.Count; i++)
            if (!items[i].Equals(data.items[i])) return false;
        return true;
    }

    public string Print() => $"PackageInfo: {orderID.Print()}, {shipmentInfo.Print()}, items = {string.Join(", ", items.Select(x => x.Print()))}";
}
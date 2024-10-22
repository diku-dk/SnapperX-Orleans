using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;

namespace MarketPlace.Grains.LogState;

[GenerateSerializer]
[MessagePackObject]
public class PackageLine : AbstractValue<PackageLine>, ISnapperValue
{
    public readonly OrderID orderID;

    public readonly List<OrderItem> orderItems;

    public readonly PackageID packageID;

    public readonly PackageInfo packageInfo;

    public PackageLine()
    {
        orderID = new OrderID();
        orderItems = new List<OrderItem>();
        packageID = new PackageID();
        packageInfo = new PackageInfo();
    }

    public PackageLine(OrderID orderID, List<OrderItem> orderItems, PackageID packageID, PackageInfo packageInfo)
    {
        this.orderID = orderID;
        this.orderItems = orderItems;
        this.packageID = packageID;
        this.packageInfo = packageInfo;
    }

    public object Clone() => new PackageLine((OrderID)orderID.Clone(), new List<OrderItem>(orderItems), (PackageID)packageID.Clone(), (PackageInfo)packageInfo.Clone());

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as PackageLine;
            if (data == null) return false;
            if (!orderID.Equals(data.orderID)) return false;
            if (!packageID.Equals(data.packageID)) return false;
            if (!packageInfo.Equals(data.packageInfo)) return false;
            if (orderItems.Count != data.orderItems.Count) return false;

            for (var i = 0; i < orderItems.Count; i++)
                if (!orderItems[i].Equals(data.orderItems[i])) return false;

            return true;
        }
        catch { return false; }
    }

    public string Print() => $"PackageLine: {orderID.Print()}, orderItems = {string.Join(", ", orderItems.Select(x => x.Print()))}, {packageID.Print()}, {packageInfo.Print()}";
}
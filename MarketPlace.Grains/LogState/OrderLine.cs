using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;

namespace MarketPlace.Grains.LogState;

[GenerateSerializer]
[MessagePackObject]
public class OrderLine : AbstractValue<OrderLine>, ISnapperValue
{
    public readonly OrderID orderID;

    public readonly OrderInfo orderInfo;

    public readonly Dictionary<PackageID, PackageInfo> packages;

    public OrderLine() { orderID = new OrderID(); orderInfo = new OrderInfo(); packages = new Dictionary<PackageID, PackageInfo>(); }

    public OrderLine(OrderID orderID, OrderInfo orderInfo, Dictionary<PackageID, PackageInfo> packages)
    {
        this.orderID = orderID;
        this.orderInfo = orderInfo;
        this.packages = packages;
    }

    public object Clone() => new OrderLine((OrderID)orderID.Clone(), (OrderInfo)orderInfo.Clone(), new Dictionary<PackageID, PackageInfo>(packages));

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as OrderLine;
            if (data == null) return false;
            if (!orderID.Equals(data.orderID)) return false;
            if (!orderInfo.Equals(data.orderInfo)) return false;
            if (packages.Count != data.packages.Count) return false;

            foreach (var item in packages)
            {
                if (!data.packages.ContainsKey(item.Key)) return false;
                if (!data.packages[item.Key].Equals(item.Value)) return false;
            }

            return true;
        }
        catch { return false; }
    }

    public string Print() => $"OrderLine: {orderID.Print()}, {orderInfo.Print()}, {string.Join(", ", packages.Select(x => $"{x.Key.Print()} => {x.Value.Print()}"))}";
}
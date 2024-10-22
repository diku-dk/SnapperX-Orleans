using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class OrderInfo : AbstractValue<OrderInfo>, ISnapperValue, IEquatable<OrderInfo>
{
    [Key(0)]
    [Id(0)]
    public readonly PaymentInfo paymentInfo;

    [Key(1)]
    [Id(1)]
    public readonly List<OrderItem> items;

    [Key(2)]
    [Id(2)]
    public List<PackageID> packageIDs;

    public OrderInfo() { paymentInfo = new PaymentInfo(); items = new List<OrderItem>();  packageIDs = new List<PackageID>(); }

    public OrderInfo(PaymentInfo paymentInfo, List<OrderItem> items, List<PackageID> packageIDs)
    {
        this.paymentInfo = paymentInfo;
        this.items = items;
        this.packageIDs = packageIDs;
    }

    public object Clone() => new OrderInfo((PaymentInfo)paymentInfo.Clone(), new List<OrderItem>(items), new List<PackageID>(packageIDs));

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as OrderInfo;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(OrderInfo? data)
    {
        if (data == null) return false;
        if (!paymentInfo.Equals(data.paymentInfo)) return false;
        if (items.Count != data.items.Count) return false;
        if (packageIDs.Count != data.packageIDs.Count) return false;

        for (var i = 0; i < items.Count; i++)
            if (!items[i].Equals(data.items[i])) return false;

        for (var i = 0; i < packageIDs.Count; i++)
            if (!packageIDs[i].Equals(data.packageIDs[i])) return false;

        return true;
    }

    public string Print() => $"OrderInfo: {paymentInfo.Print()}, items = {string.Join(", ", items.Select(x => x.Print()))}, packageIDs = {string.Join(", ", packageIDs.Select(x => x.Print()))}";
}
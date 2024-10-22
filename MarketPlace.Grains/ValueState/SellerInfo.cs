using Concurrency.Common.State;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class SellerInfo : AbstractValue<SellerInfo>, ISnapperValue, IEquatable<SellerInfo>
{
    [Key(0)]
    [Id(0)]
    public MyCounter totalNumOrder;

    [Key(1)]
    [Id(1)]
    public MyCounter totalNumProduct;

    public SellerInfo() { totalNumOrder = new MyCounter(); totalNumProduct = new MyCounter(); }

    public SellerInfo(MyCounter totalNumOrder, MyCounter totalNumProduct) { this.totalNumOrder = totalNumOrder; this.totalNumProduct = totalNumProduct; }

    public object Clone() => new SellerInfo(new MyCounter(totalNumOrder.Get()), new MyCounter(totalNumProduct.Get()));

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as SellerInfo;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(SellerInfo? data)
    {
        if (data == null) return false;
        return totalNumProduct.Equals(data.totalNumProduct) && totalNumProduct.Equals(data.totalNumProduct);
    }

    public string Print() => $"SellerInfo: totalNumOrder = {totalNumOrder.Get()}, totalNumProduct = {totalNumProduct.Get()}";
}
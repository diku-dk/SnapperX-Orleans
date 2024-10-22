using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class StockInfo : AbstractValue<StockInfo>, ISnapperValue, IEquatable<StockInfo>
{
    [Key(0)]
    [Id(0)]
    public long quantity;

    [Key(1)]
    [Id(1)]
    public string address;

    public StockInfo() { quantity = -1; address = "default"; }

    public StockInfo(long quantity, string address)
    {
        this.quantity = quantity;
        this.address = address;
    }

    public object Clone() => new StockInfo(quantity, (string)address.Clone());

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as StockInfo;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(StockInfo? data)
    {
        if (data == null) return false;
        return quantity == data.quantity && address == data.address;
    }

    public string Print() => $"StockInfo: quantity = {quantity}, address = {address}";
}
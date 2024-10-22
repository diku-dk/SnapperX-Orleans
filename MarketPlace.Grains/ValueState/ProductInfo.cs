using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class ProductInfo : AbstractValue<ProductInfo>, ISnapperValue, IEquatable<ProductInfo>
{
    [Id(0)]
    [Key(0)]
    public string name;

    [Id(1)]
    [Key(1)]
    public double price;

    public ProductInfo() { name = "default"; price = -1; }

    public ProductInfo(string name, double price) { this.name = name; this.price = price; }

    public object Clone() => new ProductInfo((string)name.Clone(), price);

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as ProductInfo;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(ProductInfo? other)
    {
        if (other == null) return false;
        return name == other.name && price == other.price;
    }

    public string Print() => $"ProductInfo: name = {name}, price = {price}";
}
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

/// <summary> this is a snapshot of the bought item, like its price etc </summary>
[MessagePackObject]
[GenerateSerializer]
public class OrderItem : AbstractValue<OrderItem>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public readonly ProductID productID;

    [Key(1)]
    [Id(1)]
    public readonly ProductInfo productInfo;

    [Key(2)]
    [Id(2)]
    public readonly int quantity;

    [Key(3)]
    [Id(3)]
    public readonly int deliveryCityID;

    public OrderItem() { productID = new ProductID(); productInfo = new ProductInfo(); quantity = -1; deliveryCityID = -1; }

    public OrderItem(ProductID productID, ProductInfo productInfo, int quantity, int deliveryCityID)
    {
        this.productID = productID;
        this.productInfo = productInfo;
        this.quantity = quantity;
        this.deliveryCityID = deliveryCityID;
    }

    public object Clone() => new OrderItem((ProductID)productID.Clone(), (ProductInfo)productInfo.Clone(), quantity, deliveryCityID);

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as OrderItem;
            if (data == null) return false;
            return productID.Equals(data.productID) && productInfo.Equals(data.productInfo) && quantity == data.quantity && deliveryCityID == data.deliveryCityID;
        }
        catch { return false; }
    }

    public string Print() => $"OrderItem: {productID.Print()}, {productInfo.Print()}, quantity = {quantity}, deliveryCityID = {deliveryCityID}";
}
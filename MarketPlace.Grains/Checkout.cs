using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;

namespace MarketPlace.Grains;

[MessagePackObject]
[GenerateSerializer]
public class Checkout
{
    [Id(0)]
    [Key(0)]
    public readonly Address deliveryAddress;

    [Id(1)]
    [Key(1)]
    public readonly PaymentMethod paymentMethod;

    [Id(2)]
    [Key(2)]
    public readonly Dictionary<ProductID, int> items;     // productID, quantity

    [Id(3)]
    [Key(3)]
    public Dictionary<ProductID, ProductInfo> infoPerProduct;

    public Checkout() 
    {
        deliveryAddress = new Address();
        paymentMethod = new PaymentMethod();
        items = new Dictionary<ProductID, int>();
        infoPerProduct = new Dictionary<ProductID, ProductInfo>();
    }

    public Checkout(Address deliveryAddress, PaymentMethod paymentMethod, Dictionary<ProductID, int> items)
    {
        this.deliveryAddress = deliveryAddress;
        this.paymentMethod = paymentMethod;
        this.items = items;
        infoPerProduct = new Dictionary<ProductID, ProductInfo>();
    }
}
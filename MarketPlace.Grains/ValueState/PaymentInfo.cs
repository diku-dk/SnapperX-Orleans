using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class PaymentInfo : AbstractValue<PaymentInfo>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public readonly CustomerID customerID;

    [Key(1)]
    [Id(1)]
    public readonly DateTime timestamp;

    [Key(2)]
    [Id(2)]
    public readonly PaymentMethod paymentMethod;

    [Key(3)]
    [Id(3)]
    public readonly string detail;

    public PaymentInfo() { customerID = new CustomerID(); timestamp = DateTime.MinValue; paymentMethod = new PaymentMethod();  detail = ""; }

    public PaymentInfo(CustomerID customerID, DateTime timestamp, PaymentMethod paymentMethod, string detail)
    {
        this.customerID = customerID;
        this.timestamp = timestamp;
        this.paymentMethod = paymentMethod;
        this.detail = detail;
    }

    public object Clone() => new PaymentInfo((CustomerID)customerID.Clone(), timestamp, (PaymentMethod)paymentMethod.Clone(), (string)detail.Clone());

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as PaymentInfo;
            if (data == null) return false;
            return customerID.Equals(data.customerID) && timestamp == data.timestamp && paymentMethod.Equals(data.paymentMethod) && detail == data.detail;
        }
        catch { return false; }
    }

    public string Print() => $"PaymentInfo: {customerID.Print()}, timestamp = {timestamp}, {paymentMethod.Print()}, detail = {detail}";
}
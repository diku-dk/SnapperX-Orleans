using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class PaymentMethod : AbstractValue<PaymentMethod>, ISnapperValue, IEquatable<PaymentMethod>
{
    [Key(0)]
    [Id(0)]
    public readonly int accountID;

    [Key(1)]
    [Id(1)]
    public readonly string paymentService;

    public PaymentMethod() { accountID = -1; paymentService = ""; }

    public PaymentMethod(int accountID, string paymentService) { this.accountID = accountID; this.paymentService = paymentService; }

    public object Clone() => new PaymentMethod(accountID, (string)paymentService.Clone());

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as PaymentMethod;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(PaymentMethod? data)
    {
        if (data == null) return false;
        return accountID == data.accountID && paymentService == data.paymentService;
    }

    public string Print() => $"PaymentMethod: accountID = {accountID}, paymentService = {paymentService}";
}
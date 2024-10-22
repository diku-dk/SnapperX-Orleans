using Concurrency.Common.State;
using MessagePack;

namespace SmallBank.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class AccountInfo : AbstractValue<AccountInfo>, ISnapperValue, IEquatable<AccountInfo>
{
    [Key(0)]
    [Id(0)]
    public int balance { get; set; }

    public AccountInfo(int balance) => this.balance = balance;
    
    public AccountInfo() => balance = 0;

    public object Clone() => new AccountInfo(balance);

    public string Print() => $"balance = {balance}";

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as AccountInfo;
            return Equals(data);
        }
        catch (Exception)
        {
            return false;
        }
    }

    public bool Equals(AccountInfo? data)
    {
        if (data == null) return false;

        return balance == data.balance;
    }
}
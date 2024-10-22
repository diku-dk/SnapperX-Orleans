using Concurrency.Common.State;
using MessagePack;

namespace SmallBank.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class AccountLog : AbstractValue<AccountLog>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public string log;

    public AccountLog() => log = "";

    public AccountLog(string log) => this.log = log;

    public object Clone() => new AccountLog((string)log.Clone());

    public string Print() => log;

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as AccountLog;
            if (data == null) return false;

            return log == data.log;
        }
        catch  { return false; }
    }
}
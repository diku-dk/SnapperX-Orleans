using Concurrency.Common.State;
using MessagePack;

namespace TestApp.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class TestValue : AbstractValue<TestValue>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public double balance { get; set; }

    [Key(1)]
    [Id(1)]
    public double saving { get; set; }

    public TestValue(double balance, double saving)
    {
        this.balance = balance;
        this.saving = saving;
    }

    public TestValue() { balance = 0; saving = 0; }

    public object Clone() => new TestValue(balance, saving);

    public string Print() => $"TestValue: balance = {balance}, saving = {saving}";

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as TestValue;
            if (data == null) return false;

            return balance == data.balance && saving == data.saving;
        }
        catch { return false; }
    }
}
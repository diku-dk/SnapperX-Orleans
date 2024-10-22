using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class GeneralLongValue : AbstractValue<GeneralLongValue>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public long value;

    public GeneralLongValue() { value = -1; }

    public GeneralLongValue(long value) => this.value = value;

    public object Clone() => new GeneralLongValue(value);

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as GeneralLongValue;
            if (data == null) return false;
            return value == data.value;
        }
        catch { return false; }
    }

    public string Print() => $"value = {value}";
}

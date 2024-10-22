using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.KeyState;

[GenerateSerializer]
[MessagePackObject]
public class GeneralStringKey : AbstractKey<GeneralStringKey>, ISnapperKey, IEquatable<GeneralStringKey>
{
    [Key(0)]
    [Id(0)]
    public readonly string key;

    public GeneralStringKey() { key = ""; }

    public GeneralStringKey(string key) { this.key = key; }

    public object Clone() => new GeneralStringKey((string)key.Clone());

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as GeneralStringKey;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(GeneralStringKey? other)
    {
        if (other == null) return false;
        return key == other.key;
    }

    public override int GetHashCode() => key.GetHashCode();

    public string Print() => $"key = {key}";
}
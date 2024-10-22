using Concurrency.Common.State;
using MessagePack;

namespace TestApp.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class TestKeyV1 : AbstractKey<TestKeyV1>, ISnapperKey
{
    [Key(0)]
    [Id(0)]
    public readonly int Id;

    [Key(1)]
    [Id(1)]
    public readonly string Name;

    public TestKeyV1(int Id, string Name) { this.Id = Id; this.Name = Name; }

    public TestKeyV1() { Id = -1; Name = "default"; }

    public object Clone() => new TestKeyV1(Id, (string)Name.Clone());

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = (TestKeyV1)other;
            if (data == null) return false;
            return Id == data.Id && Name == data.Name;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public override int GetHashCode() => Id.GetHashCode() ^ Name.GetHashCode();

    public string Print() => $"TestKeyV1: Id = {Id}, Name = {Name}";
}
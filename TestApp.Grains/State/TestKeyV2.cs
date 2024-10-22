using Concurrency.Common.State;
using MessagePack;

namespace TestApp.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class TestKeyV2 : AbstractKey<TestKeyV2>, ISnapperKey
{
    [Key(0)]
    [Id(0)]
    public readonly int id;

    [Key(1)]
    [Id(1)]
    public readonly string info;

    [Key(2)]
    [Id(2)]
    public readonly string note;

    public TestKeyV2(int id, string info, string note) { this.id = id; this.info = info; this.note = note; }

    public TestKeyV2() { id = -1; info = ""; note = ""; }

    public object Clone() => new TestKeyV2(id, (string)info.Clone(), (string)note.Clone());

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = (TestKeyV2)other;
            if (data == null) return false;
            else return id == data.id && info == data.info && note == data.note;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public override int GetHashCode() => id.GetHashCode() ^ info.GetHashCode() ^ note.GetHashCode();

    public string Print() => $"TestKeyV2: id = {id}, info = {info}, note = {note}";
}
using Concurrency.Common.State;
using MessagePack;

namespace TestApp.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class TestLog : AbstractValue<TestLog>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public string log;

    public TestLog() => log = "";

    public TestLog(string log) => this.log = log;

    public object Clone() => new TestLog((string)log.Clone());

    public string Print() => log;

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as TestLog;
            if (data == null) return false;

            return log == data.log;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
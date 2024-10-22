using MessagePack;

namespace Utilities;

[GenerateSerializer]
[MessagePackObject]
public class MyCounter : IEquatable<MyCounter>, ICloneable
{
    [Key(0)]
    [Id(0)]
    int count;

    public MyCounter(int count) => this.count = count;

    public MyCounter() { count = 0; }

    public void Add(int n) => count += n;

    public int Get() => count;

    public void Deduct(int n) => count -= n;

    public bool Equals(MyCounter? other) => other != null && count == other.count;

    public object Clone() => new MyCounter(count);
}
namespace Concurrency.Common.State;

public interface ISnapperKey : ICloneable, IEquatable<ISnapperKey>
{
    public string Print();

    public int GetHashCode();

    public byte[] Serialize(ISnapperKey data);

    public ISnapperKey Deserialize(byte[] bytes);

    public string? GetAssemblyQualifiedName();
}
namespace Concurrency.Common.State;

public interface ISnapperValue : ICloneable, IEquatable<ISnapperValue>
{
    public string Print();

    public byte[] Serialize(ISnapperValue data);

    public ISnapperValue Deserialize(byte[] bytes);

    public string? GetAssemblyQualifiedName();
}
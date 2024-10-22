using MessagePack;

namespace Concurrency.Common.State;

[GenerateSerializer]
[MessagePackObject]
public abstract class AbstractValue<T> where T : ISnapperValue
{
    public byte[] Serialize(ISnapperValue data) => MessagePackSerializer.Serialize((T)data);

    public ISnapperValue Deserialize(byte[] bytes) => MessagePackSerializer.Deserialize<T>(bytes);

    public string? GetAssemblyQualifiedName() => typeof(T).AssemblyQualifiedName;
}

[GenerateSerializer]
[MessagePackObject]
public abstract class AbstractKey<T> where T : ISnapperKey
{
    public byte[] Serialize(ISnapperKey data) => MessagePackSerializer.Serialize((T)data);

    public ISnapperKey Deserialize(byte[] bytes) => MessagePackSerializer.Deserialize<T>(bytes);

    public string? GetAssemblyQualifiedName() => typeof(T).AssemblyQualifiedName;
}
using MessagePack;
using System.Diagnostics;

namespace Concurrency.Common.State;

/// <summary> this function is applied to a key that has UpdateReference on another key </summary>
public interface IUpdateFunction : IEquatable<IUpdateFunction>, ICloneable
{
    /// <summary> apply update to key2 based on the update happened on key1 </summary>
    public ISnapperValue ApplyUpdate(ISnapperKey key1, ISnapperValue oldValue, ISnapperValue newValue, ISnapperKey key2, ISnapperValue value2);

    public byte[] Serialize(IUpdateFunction data);

    public IUpdateFunction Deserialize(byte[] bytes);

    public string? GetAssemblyQualifiedName();
}

/// <summary> this function is a place-holder for delete reference </summary>
[MessagePackObject]
[GenerateSerializer]
public class DefaultFunction : IUpdateFunction
{
    public DefaultFunction() { }

    /// <summary> this function does not do any change to the value 2 </summary>
    public ISnapperValue ApplyUpdate(ISnapperKey key1, ISnapperValue oldValue, ISnapperValue newValue, ISnapperKey key2, ISnapperValue value2) => value2;

    public byte[] Serialize(IUpdateFunction data)
    {
        var func = data as DefaultFunction;
        if (func == null) throw new Exception($"The input function is not with type DefaultUpdateFunction");

        return MessagePackSerializer.Serialize(func);
    }

    public IUpdateFunction Deserialize(byte[] bytes) => MessagePackSerializer.Deserialize<DefaultFunction>(bytes);

    public string? GetAssemblyQualifiedName() => typeof(DefaultFunction).AssemblyQualifiedName;

    public bool Equals(IUpdateFunction? other)
    {
        if (other == null) return false;
        var func = other as DefaultFunction;
        return func != null;
    }

    public object Clone() => new DefaultFunction();
}

[MessagePackObject]
[GenerateSerializer]
public class DefaultUpdateFunction : IUpdateFunction
{
    public DefaultUpdateFunction() { }

    /// <summary> over-write the value2 to the latest value of key1 </summary>
    public ISnapperValue ApplyUpdate(ISnapperKey key1, ISnapperValue oldValue, ISnapperValue newValue, ISnapperKey key2, ISnapperValue value2)
    {
        Debug.Assert(key1.Equals(key2));
        return newValue;
    }

    public byte[] Serialize(IUpdateFunction data)
    {
        var func = data as DefaultUpdateFunction;
        if (func == null) throw new Exception($"The input function is not with type DefaultUpdateFunction");

        return MessagePackSerializer.Serialize(func);
    }

    public IUpdateFunction Deserialize(byte[] bytes) => MessagePackSerializer.Deserialize<DefaultUpdateFunction>(bytes);

    public string? GetAssemblyQualifiedName() => typeof(DefaultUpdateFunction).AssemblyQualifiedName;

    public bool Equals(IUpdateFunction? other)
    {
        if (other == null) return false;
        var func = other as DefaultUpdateFunction;
        return func != null;
    }

    public object Clone() => new DefaultUpdateFunction();
}

[MessagePackObject]
[GenerateSerializer]
public class TestUpdateFunction : IUpdateFunction
{
    public TestUpdateFunction() { }

    public ISnapperValue ApplyUpdate(ISnapperKey key1, ISnapperValue oldValue, ISnapperValue newValue, ISnapperKey key2, ISnapperValue value2)
    {
        Debug.Assert(key1.Equals(key2));
        return newValue;
    }

    public byte[] Serialize(IUpdateFunction data)
    {
        var func = data as TestUpdateFunction;
        if (func == null) throw new Exception($"The input function is not with type TestUpdateFunction");

        return MessagePackSerializer.Serialize(func);
    }

    public IUpdateFunction Deserialize(byte[] bytes) => MessagePackSerializer.Deserialize<TestUpdateFunction>(bytes);

    public string? GetAssemblyQualifiedName() => typeof(TestUpdateFunction).AssemblyQualifiedName;

    public bool Equals(IUpdateFunction? other)
    {
        if (other == null) return false;
        var func = other as TestUpdateFunction;
        return func != null;
    }

    public object Clone() => new TestUpdateFunction();
}
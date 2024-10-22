using MessagePack;
using System.Diagnostics;
using System.Reflection;
using Utilities;

namespace Concurrency.Common.State;

public static class SnapperSerializer
{
    public static byte[] Serialize(Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> references)
    {
        var list = new List<(byte[], byte[])>();
        foreach (var item in references) list.Add((MessagePackSerializer.Serialize(item.Key), Serialize(item.Value)));
        return MessagePackSerializer.Serialize(list);
    }

    public static Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> DeserializeAllReferences(byte[] bytes)
    {
        var references = new Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>();
        var list = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes);
        foreach (var item in list) references.Add(MessagePackSerializer.Deserialize<SnapperKeyReferenceType>(item.Item1), DeserializeReferences(item.Item2));
        return references;
    }

    public static byte[] Serialize(Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>> references)
    {
        var list = new List<(byte[], byte[], byte[], byte[])>();    // key1, grain2, key2, func
        foreach (var item in references)
        {
            var key1 = item.Key;
            foreach (var iitem in item.Value)
            {
                var grain2 = iitem.Key;
                foreach (var iiitem in iitem.Value)
                {
                    var key2 = iiitem.Key;
                    var function = iiitem.Value;
                    list.Add((Serialize(key1), MessagePackSerializer.Serialize(grain2), Serialize(key2), Serialize(function)));
                }   
            }
        }

        return MessagePackSerializer.Serialize(list);
    }

    public static Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>> DeserializeReferences(byte[] bytes)
    {
        var list = MessagePackSerializer.Deserialize<List<(byte[], byte[], byte[], byte[])>>(bytes);
        var references = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        foreach (var item in list)
        {
            var key1 = DeserializeSnapperKey(item.Item1);
            var grain2 = MessagePackSerializer.Deserialize<GrainID>(item.Item2);
            var key2 = DeserializeSnapperKey(item.Item3);
            var function = DeserializeUpdateFunction(item.Item4);

            if (!references.ContainsKey(key1)) references.Add(key1, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());
            if (!references[key1].ContainsKey(grain2)) references[key1].Add(grain2, new Dictionary<ISnapperKey, IUpdateFunction>());
            references[key1][grain2].Add(key2, function);
        }
        return references;
    }

    public static byte[] Serialize(ISnapperKey key)
    {
        var item = (
            key.Serialize(key),
            MessagePackSerializer.Serialize(key.GetAssemblyQualifiedName()));
        return MessagePackSerializer.Serialize(item);
    }

    public static ISnapperKey DeserializeSnapperKey(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[])>(bytes);

        var keyTypeName = MessagePackSerializer.Deserialize<string>(item.Item2);

        var key = GetInstance(keyTypeName, item.Item1) as ISnapperKey;
        Debug.Assert(key != null);
        return key;
    }

    public static byte[] Serialize(List<SnapperRecord> updates)
    {
        var list = updates.Select(Serialize).ToList();
        return MessagePackSerializer.Serialize(list);
    }

    public static List<SnapperRecord> DeserializeUpdatesOnList(byte[] bytes)
    {
        var list = MessagePackSerializer.Deserialize<List<byte[]>>(bytes);
        return list.Select(DeserializeSnapperRecord).ToList();
    }

    public static byte[] Serialize(SnapperRecord record)
    {
        var item = (
            record.record.Serialize(record.record),
            MessagePackSerializer.Serialize(record.recordTypeName));

        return MessagePackSerializer.Serialize(item);
    }

    public static SnapperRecord DeserializeSnapperRecord(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[])>(bytes);

        var recordTypeName = MessagePackSerializer.Deserialize<string>(item.Item2);

        var record = GetInstance(recordTypeName, item.Item1) as ISnapperValue;
        Debug.Assert(record != null);

        return new SnapperRecord(record);
    }

    public static byte[] Serialize(SnapperValue value)
    {
        var serializedFunction = new byte[0];
        if (value.function != null) serializedFunction = Serialize(value.function);

        var serializedDependentGrainID = new byte[0];
        if (value.dependentGrain != null) serializedDependentGrainID = MessagePackSerializer.Serialize(value.dependentGrain);

        var serializedDependentKey = new byte[0];
        if (value.dependentKey != null) serializedDependentKey = Serialize(value.dependentKey);

        var valueTypeName = value.value.GetAssemblyQualifiedName();
        Debug.Assert(!string.IsNullOrEmpty(valueTypeName));

        var item = (
            value.value.Serialize(value.value),
            MessagePackSerializer.Serialize(value.keyTypeName),
            MessagePackSerializer.Serialize(valueTypeName),
            MessagePackSerializer.Serialize(value.referenceType),
            serializedFunction,
            serializedDependentGrainID,
            serializedDependentKey);

        return MessagePackSerializer.Serialize(item);
    }

    public static SnapperValue DeserializeSnapperValue(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[], byte[], byte[], byte[], byte[], byte[])>(bytes);

        var keyTypeName = MessagePackSerializer.Deserialize<string>(item.Item2);

        var valueTypeName = MessagePackSerializer.Deserialize<string>(item.Item3);

        var referenceType = MessagePackSerializer.Deserialize<SnapperKeyReferenceType>(item.Item4);

        var value = GetInstance(valueTypeName, item.Item1) as ISnapperValue;
        Debug.Assert(value != null);

        IUpdateFunction? function = null;
        GrainID? dependentGrain = null;
        ISnapperKey? dependentKey = null;
        if (item.Item5.Length != 0)
        {
            Debug.Assert(item.Item6.Length != 0 && item.Item7.Length != 0);
            function = DeserializeUpdateFunction(item.Item5);
            dependentGrain = MessagePackSerializer.Deserialize<GrainID>(item.Item6);
            dependentKey = DeserializeSnapperKey(item.Item7);
        }
        else Debug.Assert(item.Item6.Length == 0 && item.Item7.Length == 0);

        return new SnapperValue(value, keyTypeName, referenceType, function, dependentGrain, dependentKey);
    }

    public static byte[] Serialize(ISnapperValue? value)
    {
        var item = (new byte[0], new byte[0]);

        if (value != null)
        {
            var valueTypeName = value.GetAssemblyQualifiedName();
            Debug.Assert(!string.IsNullOrEmpty(valueTypeName));

            item = (
                MessagePackSerializer.Serialize(valueTypeName),
                value.Serialize(value));
        }

        return MessagePackSerializer.Serialize(item);
    }

    public static ISnapperValue? DeserializeISnapperValue(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[])>(bytes);

        if (item.Item1.Length == 0)
        {
            Debug.Assert(item.Item2.Length == 0);
            return null;
        }

        var valueTypeName = MessagePackSerializer.Deserialize<string>(item.Item1);
        Debug.Assert(!string.IsNullOrEmpty(valueTypeName));

        var value = GetInstance(valueTypeName, item.Item2) as ISnapperValue;
        Debug.Assert(value != null);
        return value;
    }

    public static byte[] Serialize(ReferenceInfo info)
    {
        var item = (
            MessagePackSerializer.Serialize(info.referenceType),
            MessagePackSerializer.Serialize(info.grain1),
            Serialize(info.key1),
            MessagePackSerializer.Serialize(info.grain2),
            Serialize(info.key2),
            Serialize(info.function));
        return MessagePackSerializer.Serialize(item);
    }

    public static ReferenceInfo DeserializeReferenceInfo(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[], byte[], byte[], byte[], byte[])>(bytes);
        var referenceType = MessagePackSerializer.Deserialize<SnapperKeyReferenceType>(item.Item1);
        var grain1 = MessagePackSerializer.Deserialize<GrainID>(item.Item2);
        var key1 = DeserializeSnapperKey(item.Item3);
        var grain2 = MessagePackSerializer.Deserialize<GrainID>(item.Item4);
        var key2 = DeserializeSnapperKey(item.Item5);
        var function = DeserializeUpdateFunction(item.Item6);
        return new ReferenceInfo(referenceType, grain1, key1, grain2, key2, function);
    }

    public static byte[] Serialize(List<(UpdateType, ReferenceInfo)> updates)
    {
        var list = updates.Select(x => (MessagePackSerializer.Serialize(x.Item1), Serialize(x.Item2)));
        return MessagePackSerializer.Serialize(list);
    }

    public static List<(UpdateType, ReferenceInfo)> DeserializeUpdatesOnReference(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes);
        return item.Select(x => (MessagePackSerializer.Deserialize<UpdateType>(x.Item1), DeserializeReferenceInfo(x.Item2))).ToList();
    }

    public static byte[] Serialize(List<Update> updates)
    {
        var list = updates.Select(Serialize).ToList();
        return MessagePackSerializer.Serialize(list);
    }

    public static List<Update> DeserializeUpdatesOnDictionary(byte[] bytes)
    {
        var updates = MessagePackSerializer.Deserialize<List<byte[]>>(bytes);
        return updates.Select(DeserializeUpdate).ToList();
    }

    public static byte[] Serialize(Update update)
    {
        var serializedOldValue = new byte[0];
        if (update.oldValue != null) serializedOldValue = Serialize(update.oldValue);

        var item = (
            MessagePackSerializer.Serialize(update.isForwarded),
            MessagePackSerializer.Serialize(update.updateType),
            update.key.Serialize(update.key),
            Serialize(update.value),
            serializedOldValue);

        return MessagePackSerializer.Serialize(item);
    }

    public static Update DeserializeUpdate(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[], byte[], byte[], byte[])>(bytes);

        var isForwarded = MessagePackSerializer.Deserialize<bool>(item.Item1);

        var updateType = MessagePackSerializer.Deserialize<UpdateType>(item.Item2);

        var value = DeserializeSnapperValue(item.Item4);

        var key = GetInstance(value.keyTypeName, item.Item3) as ISnapperKey;
        Debug.Assert(key != null);

        ISnapperValue? oldValue = null;
        if (item.Item5.Length != 0) oldValue = DeserializeISnapperValue(item.Item5);

        return new Update(isForwarded, updateType, key, value, oldValue);
    }

    public static byte[] Serialize(IUpdateFunction function)
    {
        var item = (
            function.Serialize(function),
            MessagePackSerializer.Serialize(function.GetAssemblyQualifiedName()));
        return MessagePackSerializer.Serialize(item);
    }

    public static IUpdateFunction DeserializeUpdateFunction(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[])>(bytes);
        var funcType = MessagePackSerializer.Deserialize<string>(item.Item2);
        var func = GetInstance(funcType, item.Item1) as IUpdateFunction;
        Debug.Assert(func != null);
        return func;
    }

    public static byte[] Serialize(ForwardedUpdate forwardedUpdate)
    {
        var serializedFunction = new byte[0];
        if (forwardedUpdate.function != null) serializedFunction = Serialize(forwardedUpdate.function);

        var item = (
            MessagePackSerializer.Serialize(forwardedUpdate.toFollower),
            Serialize(forwardedUpdate.update),
            MessagePackSerializer.Serialize(forwardedUpdate.referenceType),
            MessagePackSerializer.Serialize(forwardedUpdate.affectedGrain),
            Serialize(forwardedUpdate.affectedKey),
            serializedFunction);
        return MessagePackSerializer.Serialize(item);
    }

    public static ForwardedUpdate DeserializeForwardedUpdate(byte[] bytes)
    {
        var item = MessagePackSerializer.Deserialize<(byte[], byte[], byte[], byte[], byte[], byte[])>(bytes);
        var toFollower = MessagePackSerializer.Deserialize<bool>(item.Item1);
        var update = DeserializeUpdate(item.Item2);
        var referenceType = MessagePackSerializer.Deserialize<SnapperKeyReferenceType>(item.Item3);
        var affectedGrain = MessagePackSerializer.Deserialize<GrainID>(item.Item4);
        var affectedKey = DeserializeSnapperKey(item.Item5);

        IUpdateFunction? function = null;
        if (item.Item6 != null) function = DeserializeUpdateFunction(item.Item6);
        return new ForwardedUpdate(toFollower, update, referenceType, affectedGrain, affectedKey, function);
    }

    public static byte[] Serialize(Dictionary<ISnapperKey, SnapperValue> items)
    {
        var list = new List<(byte[], byte[])>();
        foreach (var item in items)
        {
            var item1 = Serialize(item.Key);
            var item2 = Serialize(item.Value);
            list.Add((item1, item2));
        }
        return MessagePackSerializer.Serialize(list);
    }

    public static Dictionary<ISnapperKey, SnapperValue> DeserializeDictionary(byte[] bytes)
    {
        var list = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes);
        var items = new Dictionary<ISnapperKey, SnapperValue>();
        foreach (var item in list)
        {
            var item1 = DeserializeSnapperKey(item.Item1);
            var item2 = DeserializeSnapperValue(item.Item2);
            items.Add(item1, item2);
        }
        return items;
    }

    /// <returns> deserialize into an instance of the required type </returns>
    public static object GetInstance(string? typeName, byte[]? bytes)
    {
        Debug.Assert(typeName != null);
        Debug.Assert(bytes != null);
        Type? type = Type.GetType(typeName);
        if (type == null) Console.WriteLine($"type name = {typeName}");
        Debug.Assert(type != null);

        var keyConstructor = type.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null, CallingConventions.HasThis, new Type[0], null);
        Debug.Assert(keyConstructor != null);

        var keyInstance = keyConstructor.Invoke(null);
        Debug.Assert(keyInstance != null);

        var deSerializeMethod = type.GetMethod("Deserialize");
        Debug.Assert(deSerializeMethod != null);
        var key = deSerializeMethod.Invoke(keyInstance, new object[] { bytes });
        Debug.Assert(key != null);

        return key;
    }
}
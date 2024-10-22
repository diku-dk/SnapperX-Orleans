using Concurrency.Common.State;
using MessagePack;

namespace Concurrency.Common;

[GenerateSerializer]
public class ActorAccessInfo : ICloneable
{
    /// <summary> region ID, silo ID, grain ID (guid "+" className) </summary>
    [Id(0)]
    public readonly Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>> info;

    [Id(1)]
    public readonly Dictionary<SnapperGrainID, HashSet<ISnapperKey>> keyToAccessPerGrain;

    public ActorAccessInfo()
    {
        info = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        keyToAccessPerGrain = new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>();
    }

    public ActorAccessInfo(Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>> info, Dictionary<SnapperGrainID, HashSet<ISnapperKey>>? keyToAccessPerGrain = null)
    {
        this.info = info;
        if (keyToAccessPerGrain != null) this.keyToAccessPerGrain = keyToAccessPerGrain;
        else this.keyToAccessPerGrain = new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>();
    }

    public List<string> GetAccessRegions() => info.Keys.ToList();

    public List<string> GetAccssSilos(string regionID) => info[regionID].Keys.ToList();

    public HashSet<SnapperGrainID> GetAccessGrains(string regionID, string siloID) => info[regionID][siloID];

    public HashSet<SnapperGrainID> GetAllAccessGrains()
    {
        var grains = new HashSet<SnapperGrainID>();
        foreach (var regionInfo in info) foreach (var siloInfo in regionInfo.Value) grains.UnionWith(siloInfo.Value);
        return grains;
    }

    public bool IsEmpty() => info.Count == 0 && keyToAccessPerGrain.Count == 0;

    public object Clone() => new ActorAccessInfo(new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>(info), new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>(keyToAccessPerGrain));

    public static byte[] Serialize(ActorAccessInfo actorAccessInfo)
    {
        var item1 = new List<(byte[], byte[], byte[])>();
        foreach (var item in actorAccessInfo.info)
        {
            var regionID = MessagePackSerializer.Serialize(item.Key);
            foreach (var iitem in item.Value)
            {
                var siloID = MessagePackSerializer.Serialize(iitem.Key);
                foreach (var iiitem in iitem.Value)
                {
                    item1.Add((regionID, siloID, MessagePackSerializer.Serialize(iiitem)));
                }
            }
        }

        var item2 = new List<(byte[], List<byte[]>)>();
        foreach (var item in actorAccessInfo.keyToAccessPerGrain)
        {
            var list = new List<byte[]>();
            foreach (var iitem in item.Value) list.Add(SnapperSerializer.Serialize(iitem));
            item2.Add((MessagePackSerializer.Serialize(item.Key), list));
        }
        return MessagePackSerializer.Serialize((item1, item2));
    }

    public static ActorAccessInfo Deserialize(byte[] data)
    {
        (var item1, var item2) = MessagePackSerializer.Deserialize < (List < (byte[], byte[], byte[])>, List<(byte[], List<byte[]>)>)>(data);

        var info = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        foreach (var item in item1)
        {
            var regionID = MessagePackSerializer.Deserialize<string>(item.Item1);
            var siloID = MessagePackSerializer.Deserialize<string>(item.Item2);
            var grainID = MessagePackSerializer.Deserialize<SnapperGrainID>(item.Item3);

            if (!info.ContainsKey(regionID)) info.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());
            if (!info[regionID].ContainsKey(siloID)) info[regionID].Add(siloID, new HashSet<SnapperGrainID>());
            info[regionID][siloID].Add(grainID);
        }

        var keyInfo = new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>();
        foreach (var item in item2)
        {
            var grainID = MessagePackSerializer.Deserialize<SnapperGrainID>(item.Item1);
            var list = new HashSet<ISnapperKey>();
            foreach (var iitem in item.Item2) list.Add(SnapperSerializer.DeserializeSnapperKey(iitem));

            keyInfo.Add(grainID, list);
        }

        return new ActorAccessInfo(info, keyInfo);
    } 
}
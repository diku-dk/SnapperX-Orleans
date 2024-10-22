using Concurrency.Common;
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;

[GenerateSerializer]
[MessagePackObject]
public class ProductActorState : AbstractValue<ProductActorState>, ISnapperValue, IEquatable<ProductActorState>
{
    [Key(0)]
    [Id(0)]
    public Dictionary<ProductID, ProductInfo> items;

    [Key(1)]
    [Id(1)]
    public Dictionary<ProductID, HashSet<GrainID>> followerCartActors;

    [Key(2)]
    [Id(2)]
    public Dictionary<ProductID, HashSet<GrainID>> followerStockActors;

    public ProductActorState()
    {
        items = new Dictionary<ProductID, ProductInfo>();
        followerCartActors = new Dictionary<ProductID, HashSet<GrainID>>();
        followerStockActors = new Dictionary<ProductID, HashSet<GrainID>>();
    }

    public ProductActorState(Dictionary<ProductID, ProductInfo> items, Dictionary<ProductID, HashSet<GrainID>> followerCartActors, Dictionary<ProductID, HashSet<GrainID>> followerStockActors)
    {
        this.items = items;
        this.followerCartActors = followerCartActors;
        this.followerStockActors = followerStockActors;
    }

    public object Clone()
    {
        var followerCartActorsCopy = new Dictionary<ProductID, HashSet<GrainID>>();
        foreach (var item in followerCartActors) followerCartActorsCopy.Add((ProductID)item.Key.Clone(), item.Value.Select(x => (GrainID)x.Clone()).ToHashSet());

        var followerStockActorsCopy = new Dictionary<ProductID, HashSet<GrainID>>();
        foreach (var item in followerStockActors) followerStockActorsCopy.Add((ProductID)item.Key.Clone(), item.Value.Select(x => (GrainID)x.Clone()).ToHashSet());

        return new ProductActorState(Helper.CloneDictionary(items), followerCartActorsCopy, followerStockActorsCopy);
    }

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as ProductActorState;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(ProductActorState? data)
    {
        if (data == null) return false;

        if (!Helper.CheckDictionaryEqualty(data.items, items)) return false;

        if (data.followerCartActors.Count != followerCartActors.Count) return false;
        foreach (var item in data.followerCartActors)
        {
            if (!followerCartActors.ContainsKey(item.Key)) return false;
            if (followerCartActors[item.Key].Count != item.Value.Count) return false;
            foreach (var iitem in item.Value) if (!followerCartActors[item.Key].Contains(iitem)) return false;
        }

        if (data.followerStockActors.Count != followerStockActors.Count) return false;
        foreach (var item in data.followerStockActors)
        {
            if (!followerStockActors.ContainsKey(item.Key)) return false;
            if (followerStockActors[item.Key].Count != item.Value.Count) return false;
            foreach (var iitem in item.Value) if (!followerStockActors[item.Key].Contains(iitem)) return false;
        }

        return true;
    }

    public string Print() => "ProductActorState";
}
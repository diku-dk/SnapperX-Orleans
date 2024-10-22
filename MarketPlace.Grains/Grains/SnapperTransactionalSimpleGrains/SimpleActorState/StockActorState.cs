using Concurrency.Common;
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;

[GenerateSerializer]
[MessagePackObject]
public class StockActorState : AbstractValue<StockActorState>, ISnapperValue, IEquatable<StockActorState>
{
    [Key(0)]
    [Id(0)]
    public Dictionary<ProductID, StockInfo> stocks;

    [Key(1)]
    [Id(1)]
    public Dictionary<ProductID, GrainID> dependencies;

    public StockActorState()
    {
        stocks = new Dictionary<ProductID, StockInfo>();
        dependencies = new Dictionary<ProductID, GrainID>();
    }

    public StockActorState(Dictionary<ProductID, StockInfo> stocks, Dictionary<ProductID, GrainID> dependencies)
    {
        this.stocks = stocks;
        this.dependencies = dependencies;
    }

    public object Clone() => new StockActorState(Helper.CloneDictionary(stocks), Helper.CloneDictionary(dependencies));

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as StockActorState;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(StockActorState? data)
    {
        if (data == null) return false;

        if (!Helper.CheckDictionaryEqualty(data.stocks, stocks)) return false;
        if (!Helper.CheckDictionaryEqualty(data.dependencies, dependencies)) return false;
        return true;
    }

    public string Print() => "StockActorState";
}
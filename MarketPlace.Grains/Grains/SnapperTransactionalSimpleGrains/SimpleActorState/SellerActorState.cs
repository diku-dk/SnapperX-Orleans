using Concurrency.Common;
using Concurrency.Common.State;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;

[GenerateSerializer]
[MessagePackObject]
public class SellerActorState : AbstractValue<SellerActorState>, ISnapperValue, IEquatable<SellerActorState>
{
    [Key(0)]
    [Id(0)]
    public MyCounter totalNumOrder;

    [Key(1)]
    [Id(1)]
    public MyCounter totalNumProduct;

    [Key(2)]
    [Id(2)]
    public HashSet<GrainID> dependencies;

    public SellerActorState() 
    { 
        totalNumOrder = new MyCounter(); 
        totalNumProduct = new MyCounter(); 
        dependencies = new HashSet<GrainID>();
    }

    public SellerActorState(MyCounter totalNumOrder, MyCounter totalNumProduct, HashSet<GrainID> dependencies) 
    { 
        this.totalNumOrder = totalNumOrder; 
        this.totalNumProduct = totalNumProduct; 
        this.dependencies = dependencies;
    }

    public object Clone() => new SellerActorState(new MyCounter(totalNumOrder.Get()), new MyCounter(totalNumProduct.Get()), new HashSet<GrainID>(dependencies));
   
    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as SellerActorState;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(SellerActorState? data)
    {
        if (data == null) return false;

        if (!totalNumOrder.Equals(data.totalNumOrder)) return false;
        if (!totalNumProduct.Equals(data.totalNumProduct)) return false;
        if (dependencies.Count != data.dependencies.Count) return false;
        foreach (var item in dependencies) if (!data.dependencies.Contains(item)) return false;
        return true;
    }

    public string Print() => "SellerActorState";
}
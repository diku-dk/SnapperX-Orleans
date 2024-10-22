using Concurrency.Common;
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;

[GenerateSerializer]
[MessagePackObject]
public class CartActorState : AbstractValue<CartActorState>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public Dictionary<ProductID, ProductInfo> cartItems;

    [Key(1)]
    [Id(1)]
    public Dictionary<ProductID, GrainID> dependencies;

    public CartActorState() 
    {
        cartItems = new Dictionary<ProductID, ProductInfo>();
        dependencies = new Dictionary<ProductID, GrainID>();
    }

    public CartActorState(Dictionary<ProductID, ProductInfo> cartItems, Dictionary<ProductID, GrainID> dependencies)
    { 
        this.cartItems = cartItems;
        this.dependencies = dependencies;
    }

    public object Clone() => new CartActorState(Helper.CloneDictionary(cartItems), Helper.CloneDictionary(dependencies));
    
    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as CartActorState;
            if (data == null) return false;

            if (!Helper.CheckDictionaryEqualty(data.cartItems, cartItems)) return false;
            if (!Helper.CheckDictionaryEqualty(data.dependencies, dependencies)) return false;
            
            return true;
        }
        catch { return false; }
    }

    public string Print() => "CartActorState";
}
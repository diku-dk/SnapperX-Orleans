using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains;

public class SnapperTransactionalSimpleProductActor : TransactionExecutionGrain, IProductActor
{
    int sellerID;

    int baseCityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleProductActor");
        return myKey;
    }

    /// <summary> Each product actor contains products under a specific category, and those products belong to a specific seller </summary>
    public SnapperTransactionalSimpleProductActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log) 
        : base(typeof(SnapperTransactionalSimpleProductActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var productActorState = new ProductActorState(new Dictionary<ProductID, ProductInfo>(), new Dictionary<ProductID, HashSet<GrainID>>(), new Dictionary<ProductID, HashSet<GrainID>>());
        dictionaryState.Put(GetMyKey(), productActorState);
        return null;
    }

    public async Task<object?> ReadSimpleState(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.Read, cxt);
        var productActorState = dictionaryState.Get(GetMyKey()) as ProductActorState;
        Debug.Assert(productActorState != null);

        return MarketPlaceSerializer.SerializeProductActorState(productActorState.items, productActorState.followerCartActors, productActorState.followerStockActors);
    }

    public async Task<object?> AddProducts(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var products = obj as Dictionary<ProductID, ProductInfo>;
        Debug.Assert(products != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var productActorState = dictionaryState.Get(GetMyKey()) as ProductActorState;
        Debug.Assert(productActorState != null);

        foreach (var item in products)
        {
            if (productActorState.items.ContainsKey(item.Key)) continue;
            productActorState.items.Add(item.Key, item.Value);
        }
        
        return null;
    }

    public async Task<object?> DeleteProduct(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(!cxt.isDet);

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var productActorState = dictionaryState.Get(GetMyKey()) as ProductActorState;
        Debug.Assert(productActorState != null);

        if (!productActorState.items.ContainsKey(productID)) return null;

        var tasks = new List<Task>();

        // delete the dependency info of all cart actors
        if (productActorState.followerCartActors.ContainsKey(productID))
        {
            var cartActors = productActorState.followerCartActors[productID];
            productActorState.followerCartActors.Remove(productID);

            foreach (var cartActorID in cartActors)
            {
                var method = typeof(SnapperTransactionalSimpleCartActor).GetMethod("UpdateOrDeleteProduct");
                Debug.Assert(method != null);
                (ProductID, ProductInfo?) input = (productID, null);
                var call = new FunctionCall(method, input);
                tasks.Add(CallGrain(cartActorID, call, cxt));
            }
        }

        // delete the dependency info of all stock actors
        if (productActorState.followerStockActors.ContainsKey(productID))
        {
            var stockActors = productActorState.followerStockActors[productID];
            productActorState.followerStockActors.Remove(productID);

            foreach (var stockActorID in stockActors)
            {
                var method = typeof(SnapperTransactionalSimpleStockActor).GetMethod("DeleteProduct");
                Debug.Assert(method != null);
                var call = new FunctionCall(method, productID);
                tasks.Add(CallGrain(stockActorID, call, cxt));
            }
        }

        await Task.WhenAll(tasks);

        return null;
    }

    public async Task<object?> UpdatePrice(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(!cxt.isDet);

        Debug.Assert(obj != null);
        (var productID, var newPrice) = ((ProductID, double))obj;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var productActorState = dictionaryState.Get(GetMyKey()) as ProductActorState;
        Debug.Assert(productActorState != null);

        if (!productActorState.items.ContainsKey(productID)) return null;

        var productInfo = productActorState.items[productID];
        productInfo.price = newPrice;

        // forward updates to cart actors
        var tasks = new List<Task>();
        if (productActorState.followerCartActors.ContainsKey(productID))
        {
            var cartActors = productActorState.followerCartActors[productID];
            foreach (var cartActorID in cartActors)
            {
                var method = typeof(SnapperTransactionalSimpleCartActor).GetMethod("UpdateOrDeleteProduct");
                Debug.Assert(method != null);
                var call = new FunctionCall(method, (productID, productInfo));
                tasks.Add(CallGrain(cartActorID, call, cxt));
            }
        }
        await Task.WhenAll(tasks);

        return null;
    }

    public async Task<object?> RemoveDependencies(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var references = obj as List<(ProductID, GrainID)>;
        Debug.Assert(references != null && references.Count != 0);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var productActorState = dictionaryState.Get(GetMyKey()) as ProductActorState;
        Debug.Assert(productActorState != null);

        foreach (var item in references)
        {
            (var productID, var follower) = item;

            if (follower.className.Contains("CartActor"))
            {
                if (productActorState.followerCartActors.ContainsKey(productID))
                    productActorState.followerCartActors[productID].Remove(follower);
            }
            else if (follower.className.Contains("StockActor"))
            {
                if (productActorState.followerStockActors.ContainsKey(productID))
                    productActorState.followerStockActors[productID].Remove(follower);
            }
        }
        
        return null;
    }

    public async Task<object?> AddDependency(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var references = obj as List<(ProductID, GrainID)>;
        Debug.Assert(references != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var productActorState = dictionaryState.Get(GetMyKey()) as ProductActorState;
        Debug.Assert(productActorState != null);

        var info = new Dictionary<ProductID, ProductInfo>();
        foreach (var item in references)
        {
            (var productID, var follower) = item;

            if (!productActorState.items.ContainsKey(productID)) continue;

            info.Add(productID, productActorState.items[productID]);

            if (follower.className.Contains("CartActor"))
            {
                if (!productActorState.followerCartActors.ContainsKey(productID))
                    productActorState.followerCartActors.Add(productID, new HashSet<GrainID>());
                productActorState.followerCartActors[productID].Add(follower);
            }
            else if (follower.className.Contains("StockActor"))
            {
                if (!productActorState.followerStockActors.ContainsKey(productID))
                    productActorState.followerStockActors.Add(productID, new HashSet<GrainID>());
                productActorState.followerStockActors[productID].Add(follower);
            }
        }

        return info;
    }
}
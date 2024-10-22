using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using System.Diagnostics;

namespace MarketPlace.Grains.NonTransactionalSimpleGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class NonTransactionalSimpleProductActor : Grain, INonTransactionalSimpleProductActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;

    Dictionary<ProductID, ProductInfo> items;
    Dictionary<ProductID, HashSet<GrainID>> followerCartActors;
    Dictionary<ProductID, HashSet<GrainID>> followerStockActors;

    public NonTransactionalSimpleProductActor(ISnapperClusterCache snapperClusterCache) => this.snapperClusterCache = snapperClusterCache;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(NonTransactionalSimpleProductActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        items = new Dictionary<ProductID, ProductInfo>();
        followerCartActors = new Dictionary<ProductID, HashSet<GrainID>>();
        followerStockActors = new Dictionary<ProductID, HashSet<GrainID>>();

        return Task.FromResult(new TransactionResult());
    }

    public Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        txnResult.SetResult(MarketPlaceSerializer.SerializeProductActorState(items, followerCartActors, followerStockActors));
        return Task.FromResult(txnResult);
    }

    public Task<TransactionResult> AddProducts(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var products = obj as Dictionary<ProductID, ProductInfo>;
        Debug.Assert(products != null);

        foreach (var item in products) items.TryAdd(item.Key, item.Value);
        txnResult.AddGrain(myID);

        return Task.FromResult(txnResult);
    }

    public async Task<TransactionResult> DeleteProduct(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // delete the product
        items.Remove(productID);
        txnResult.AddGrain(myID);

        // delete the dependency info of all cart actors
        var cartActors = new HashSet<GrainID>();
        if (followerCartActors.ContainsKey(productID))
        {
            cartActors = followerCartActors[productID];
            followerCartActors.Remove(productID);
        }

        // delete the dependency info of all stock actors
        var stockActors = new HashSet<GrainID>();
        if (followerStockActors.ContainsKey(productID))
        {
            stockActors = followerStockActors[productID];
            followerStockActors.Remove(productID);
        }

        var tasks = new List<Task<TransactionResult>>();

        // forward the deletion to related cart actors
        foreach (var actor in cartActors)
        {
            var cart = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleCartActor>(snapperClusterCache, GrainFactory, myRegionID, actor);
            (ProductID, ProductInfo?) input = (productID, null);
            tasks.Add(cart.UpdateOrDeleteProduct(input));
        }

        // forward the deletion to related stock actors
        foreach (var actor in stockActors)
        {
            var stock = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleStockActor>(snapperClusterCache, GrainFactory, myRegionID, actor);
            tasks.Add(stock.DeleteProduct(productID));
        }
        await Task.WhenAll(tasks);

        foreach (var t in tasks) txnResult.MergeResult(t.Result);

        return txnResult;
    }

    public async Task<TransactionResult> UpdatePrice(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var newPrice) = ((ProductID, double))obj;
        Debug.Assert(productID != null);

        // update price
        ProductInfo? productInfo = null;
        if (items.ContainsKey(productID))
        {
            items[productID].price = newPrice;
            productInfo = items[productID];
        }
        txnResult.AddGrain(myID);

        if (productInfo == null) return txnResult;

        // find all related cart actors
        var cartActors = new HashSet<GrainID>(); 
        if (followerCartActors.ContainsKey(productID)) cartActors = followerCartActors[productID];
        
        // forward the update to all related cart actors
        var tasks = new List<Task<TransactionResult>>();
        foreach (var actor in cartActors)
        {
            var cart = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleCartActor>(snapperClusterCache, GrainFactory, myRegionID, actor);
            var input = (productID, productInfo);
            tasks.Add(cart.UpdateOrDeleteProduct(input));
        }
        await Task.WhenAll(tasks);

        foreach (var t in tasks) txnResult.MergeResult(t.Result);

        return txnResult;
    }

    public Task<TransactionResult> RemoveDependency(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var follower) = ((ProductID, GrainID))obj;
        Debug.Assert(productID != null && follower != null);

        if (follower.className.Contains("CartActor"))
            if (followerCartActors.ContainsKey(productID)) followerCartActors[productID].Remove(follower);
        else if (follower.className.Contains("StockActor")) 
                if (followerStockActors.ContainsKey(productID)) followerStockActors[productID].Remove(follower);

        txnResult.AddGrain(myID);
        return Task.FromResult(txnResult);
    }

    public Task<TransactionResult> AddDependency(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var follower) = ((ProductID, GrainID))obj;
        Debug.Assert(productID != null && follower != null);

        // check if the product exists
        ProductInfo? productInfo = null;
        if (items.ContainsKey(productID)) productInfo = items[productID];
        txnResult.AddGrain(myID);
        if (productInfo == null) return Task.FromResult(txnResult);

        // register the dependency info
        if (follower.className.Contains("CartActor"))
        {
            if (!followerCartActors.ContainsKey(productID)) followerCartActors.Add(productID, new HashSet<GrainID>());
            followerCartActors[productID].Add(follower);
        }
        else if (follower.className.Contains("StockActor"))
        {
            if (!followerStockActors.ContainsKey(productID)) followerStockActors.Add(productID, new HashSet<GrainID>());
            followerStockActors[productID].Add(follower);
        }

        // return the latest product info
        txnResult.SetResult(productInfo);
        return Task.FromResult(txnResult);
    }
}
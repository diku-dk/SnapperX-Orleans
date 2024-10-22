using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using MarketPlace.Interfaces;
using System.Diagnostics;
using Utilities;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using Orleans.Transactions.Abstractions;

namespace MarketPlace.Grains.OrleansTransactionalGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalProductActor : Grain, IOrleansTransactionalProductActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;

    readonly ITransactionalState<Dictionary<ProductID, ProductInfo>> items;
    readonly ITransactionalState<Dictionary<ProductID, HashSet<GrainID>>> followerCartActors;
    readonly ITransactionalState<Dictionary<ProductID, HashSet<GrainID>>> followerStockActors;

    public OrleansTransactionalProductActor(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(items))] ITransactionalState<Dictionary<ProductID, ProductInfo>> items,
        [TransactionalState(nameof(followerCartActors))] ITransactionalState<Dictionary<ProductID, HashSet<GrainID>>> followerCartActors,
        [TransactionalState(nameof(followerStockActors))] ITransactionalState<Dictionary<ProductID, HashSet<GrainID>>> followerStockActors)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.items = items ?? throw new ArgumentException(nameof(items));
        this.followerCartActors = followerCartActors ?? throw new ArgumentException(nameof(followerCartActors));
        this.followerStockActors = followerStockActors ?? throw new ArgumentException(nameof(followerStockActors));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalProductActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        await items.PerformUpdate(x => x = new Dictionary<ProductID, ProductInfo>());
        await followerCartActors.PerformUpdate(x => x = new Dictionary<ProductID, HashSet<GrainID>>());
        await followerStockActors.PerformUpdate(x => x = new Dictionary<ProductID, HashSet<GrainID>>());

        return new TransactionResult();
    }

    public async Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        var item1 = await items.PerformRead(x => x);
        var item2 = await followerCartActors.PerformRead(x => x);
        var item3 = await followerStockActors.PerformRead(x => x);
        txnResult.SetResult(MarketPlaceSerializer.SerializeProductActorState(item1, item2, item3));
        return txnResult;
    }

    public async Task<TransactionResult> AddProducts(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var products = obj as Dictionary<ProductID, ProductInfo>;
        Debug.Assert(products != null);

        await items.PerformUpdate(x => { foreach (var item in products) x.Add(item.Key, item.Value);});
        txnResult.AddGrain(myID);

        return txnResult;
    }

    public async Task<TransactionResult> DeleteProduct(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // delete the product
        await items.PerformUpdate(x => x.Remove(productID));
        txnResult.AddGrain(myID);

        // delete the dependency info of all cart actors
        var cartActors = await followerCartActors.PerformUpdate(x => 
        {
            var actors = new HashSet<GrainID>();
            if (x.ContainsKey(productID)) actors = x[productID];

            x.Remove(productID);

            return actors;
        });

        // delete the dependency info of all stock actors
        var stockActors = await followerStockActors.PerformUpdate(x =>
        {
            var actors = new HashSet<GrainID>();
            if (x.ContainsKey(productID)) actors = x[productID];

            x.Remove(productID);

            return actors;
        });

        var tasks = new List<Task<TransactionResult>>();

        // forward the deletion to related cart actors
        foreach (var actor in cartActors)
        {
            var cart = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalCartActor>(snapperClusterCache, GrainFactory, myRegionID, actor);
            (ProductID, ProductInfo?) input = (productID, null);
            tasks.Add(cart.UpdateOrDeleteProduct(input));
        }

        // forward the deletion to related stock actors
        foreach (var actor in stockActors)
        {
            var stock = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalStockActor>(snapperClusterCache, GrainFactory, myRegionID, actor);
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
        var productInfo = await items.PerformUpdate(x => 
        {
            if (x.ContainsKey(productID))
            {
                x[productID].price = newPrice;
                return x[productID];
            }
            else return null;
        });
        txnResult.AddGrain(myID);

        if (productInfo == null) return txnResult;

        // find all related cart actors
        var cartActors = await followerCartActors.PerformRead(x => 
        {
            if (x.ContainsKey(productID)) return x[productID];
            else return new HashSet<GrainID>();
        });

        // forward the update to all related cart actors
        var tasks = new List<Task<TransactionResult>>();
        foreach (var actor in cartActors)
        {
            var cart = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalCartActor>(snapperClusterCache, GrainFactory, myRegionID, actor);
            var input = (productID, productInfo);
            tasks.Add(cart.UpdateOrDeleteProduct(input));
        }
        await Task.WhenAll(tasks);

        foreach (var t in tasks) txnResult.MergeResult(t.Result);

        return txnResult;
    }

    /// <summary> this method is called by cart or stock actors when they delete the products </summary>
    public async Task<TransactionResult> RemoveDependency(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var follower) = ((ProductID, GrainID))obj;
        Debug.Assert(productID != null && follower != null);

        if (follower.className.Contains("CartActor"))
            await followerCartActors.PerformUpdate(x => { if (x.ContainsKey(productID)) x[productID].Remove(follower); });
        else if (follower.className.Contains("StockActor"))
            await followerStockActors.PerformUpdate(x => { if (x.ContainsKey(productID)) x[productID].Remove(follower); });
        
        txnResult.AddGrain(myID);
        return txnResult;
    }

    /// <summary> this method is called by cart or stock actors </summary>
    /// <returns> the product info or null if it does not exist </returns>
    public async Task<TransactionResult> AddDependency(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var follower) = ((ProductID, GrainID))obj;
        Debug.Assert(productID != null && follower != null);

        // check if the product exists
        var productInfo = await items.PerformUpdate(x =>
        {
            if (x.ContainsKey(productID)) return x[productID];
            else return null;
        });

        txnResult.AddGrain(myID);
        if (productInfo == null) return txnResult;

        // register the dependency info
        if (follower.className.Contains("CartActor"))
        {
            await followerCartActors.PerformUpdate(x =>
            {
                if (!x.ContainsKey(productID)) x.Add(productID, new HashSet<GrainID>());
                x[productID].Add(follower);
            });
        }
        else if (follower.className.Contains("StockActor"))
        {
            await followerStockActors.PerformUpdate(x =>
            {
                if (!x.ContainsKey(productID)) x.Add(productID, new HashSet<GrainID>());
                x[productID].Add(follower);
            });
        }

        // return the latest product info
        txnResult.SetResult(productInfo);
        return txnResult;
    }
}
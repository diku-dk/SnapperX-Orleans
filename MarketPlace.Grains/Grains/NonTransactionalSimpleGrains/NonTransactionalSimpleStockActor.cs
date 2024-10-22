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
public class NonTransactionalSimpleStockActor : Grain, INonTransactionalSimpleStockActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;
    int cityID;

    Dictionary<ProductID, StockInfo> stocks;
    Dictionary<ProductID, GrainID> dependencies;

    public NonTransactionalSimpleStockActor(ISnapperClusterCache snapperClusterCache) => this.snapperClusterCache = snapperClusterCache;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(NonTransactionalSimpleStockActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        stocks = new Dictionary<ProductID, StockInfo>();
        dependencies = new Dictionary<ProductID, GrainID>();

        return Task.FromResult(new TransactionResult());
    }

    public Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        txnResult.SetResult(MarketPlaceSerializer.SerializeStockActorState(stocks, dependencies));
        return Task.FromResult(txnResult);
    }

    public Task<TransactionResult> ReduceStock(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var items = (Dictionary<ProductID, int>)obj;
        Debug.Assert(items.Count != 0);

        var itemsReserved = new HashSet<ProductID>();
        foreach (var item in items)
        {
            if (!stocks.ContainsKey(item.Key)) continue;
            var stockInfo = stocks[item.Key];

            //if (stockInfo.quantity < item.Value) continue;
            if (stockInfo.quantity < item.Value) stockInfo.quantity += item.Value * 10;    // only for supporting PACT

            stockInfo.quantity -= item.Value;

            itemsReserved.Add(item.Key);
        }

        txnResult.AddGrain(myID);
        txnResult.SetResult(itemsReserved);
        return Task.FromResult(txnResult);
    }

    public Task<TransactionResult> ReplenishStock(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, int quantity) = ((ProductID, int))obj;

        txnResult.AddGrain(myID);
        if (!stocks.ContainsKey(productID)) return Task.FromResult(txnResult);
        
        var stockInfo = stocks[productID];
        stockInfo.quantity += quantity;

        return Task.FromResult(txnResult);
    }

    public async Task<TransactionResult> AddStock(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var stock = ((ProductID, StockInfo))obj;
        var productID = stock.Item1;

        // check if the product already exists
        txnResult.AddGrain(myID);
        if (stocks.ContainsKey(productID)) return txnResult;

        // register the dependency info in the product actor
        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(NonTransactionalSimpleProductActor).FullName);
        var productActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleProductActor>(snapperClusterCache, GrainFactory, myRegionID, productActorID);
        var res = await productActor.AddDependency((productID, myID.grainID));

        txnResult.MergeResult(res);
        if (res.resultObj == null)
        {
            txnResult.SetResult(false);
            return txnResult;
        }

        // add the stock info
        var productInfo = res.resultObj as ProductInfo;
        Debug.Assert(productInfo != null);
        stocks.TryAdd(productID, stock.Item2);

        // STEP 4: register the dependency info
        dependencies.TryAdd(productID, productActorID);

        txnResult.SetResult(true);
        return txnResult;
    }

    public Task<TransactionResult> DeleteProduct(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // delete the stock info of the product
        stocks.Remove(productID);

        // delete the dependency info
        dependencies.Remove(productID);

        txnResult.AddGrain(myID);
        return Task.FromResult(txnResult);
    }
}
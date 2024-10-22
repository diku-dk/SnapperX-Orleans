using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using Orleans.Transactions.Abstractions;
using System.Diagnostics;

namespace MarketPlace.Grains.OrleansTransactionalGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalStockActor : Grain, IOrleansTransactionalStockActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;
    int cityID;

    readonly ITransactionalState<Dictionary<ProductID, StockInfo>> stocks;
    readonly ITransactionalState<Dictionary<ProductID, GrainID>> dependencies;

    public OrleansTransactionalStockActor(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(stocks))] ITransactionalState<Dictionary<ProductID, StockInfo>> stocks,
        [TransactionalState(nameof(dependencies))] ITransactionalState<Dictionary<ProductID, GrainID>> dependencies)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.stocks = stocks ?? throw new ArgumentException(nameof(stocks)); 
        this.dependencies = dependencies ?? throw new ArgumentException(nameof(dependencies));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalStockActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        await stocks.PerformUpdate(x => x = new Dictionary<ProductID, StockInfo>());
        await dependencies.PerformUpdate(x => x = new Dictionary<ProductID, GrainID>());

        return new TransactionResult();
    }

    public async Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        var item1 = await stocks.PerformRead(x => x);
        var item2 = await dependencies.PerformRead(x => x);
        txnResult.SetResult(MarketPlaceSerializer.SerializeStockActorState(item1, item2));
        return txnResult;
    }

    /// <summary> this method is called when doing checkout trasnaction </summary>
    public async Task<TransactionResult> ReduceStock(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var items = (Dictionary<ProductID, int>)obj;
        Debug.Assert(items.Count != 0);

        var itemsReserved = new HashSet<ProductID>();
        foreach (var item in items)
        {
            var stockInfo = await stocks.PerformRead(x =>
            {
                if (x.ContainsKey(item.Key)) return x[item.Key];
                else return null;
            });
            if (stockInfo == null) continue;

            //if (stockInfo.quantity < item.Value) continue;
            if (stockInfo.quantity < item.Value) stockInfo.quantity += item.Value * 10;    // only for supporting PACT

            stockInfo.quantity -= item.Value;
            await stocks.PerformUpdate(x => x[item.Key] = stockInfo);

            itemsReserved.Add(item.Key);
        }

        txnResult.AddGrain(myID);
        txnResult.SetResult(itemsReserved);
        return txnResult;
    }

    public async Task<TransactionResult> ReplenishStock(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, int quantity) = ((ProductID, int))obj;

        var stockInfo = await stocks.PerformUpdate(x => 
        {
            if (x.ContainsKey(productID)) return x[productID];
            else return null;
        });

        txnResult.AddGrain(myID);
        
        if (stockInfo != null)
        {
            stockInfo.quantity += quantity;
            await stocks.PerformUpdate(x => x[productID] = stockInfo);
        }

        return txnResult;
    }

    public async Task<TransactionResult> AddStock(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var stock = ((ProductID, StockInfo))obj;
        var productID = stock.Item1;

        // check if the product already exists
        var productExists = await stocks.PerformRead(x => x.ContainsKey(productID));
        txnResult.AddGrain(myID);
        if (productExists) return txnResult;

        // register the dependency info in the product actor
        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(OrleansTransactionalProductActor).FullName);
        var productActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalProductActor>(snapperClusterCache, GrainFactory, myRegionID, productActorID);
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
        await stocks.PerformUpdate(x => x.Add(productID, stock.Item2));

        // STEP 4: register the dependency info
        await dependencies.PerformUpdate(x => x.Add(productID, productActorID));

        txnResult.SetResult(true);
        return txnResult;
    }

    /// <summary> this method is called when the product actor deletes a product </summary>
    public async Task<TransactionResult> DeleteProduct(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // delete the stock info of the product
        await stocks.PerformUpdate(x => x.Remove(productID));

        // delete the dependency info
        await dependencies.PerformUpdate(x => x.Remove(productID));

        txnResult.AddGrain(myID);
        return txnResult;
    }
}
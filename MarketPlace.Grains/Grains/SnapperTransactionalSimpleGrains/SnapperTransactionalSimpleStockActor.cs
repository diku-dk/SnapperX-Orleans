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

public class SnapperTransactionalSimpleStockActor : TransactionExecutionGrain, IStockActor
{
    int sellerID;

    int baseCityID;

    int cityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleStockActor");
        return myKey;
    }

    /// <summary> Each stock actor contains the stock of products under a specific category, and those products belong to a specific seller </summary>
    public SnapperTransactionalSimpleStockActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleStockActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var stockActorState = new StockActorState(new Dictionary<ProductID, StockInfo>(), new Dictionary<ProductID, GrainID>());
        dictionaryState.Put(GetMyKey(), stockActorState);
        return null;
    }

    public async Task<object?> ReadSimpleState(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.Read, cxt);
        var stockActorState = dictionaryState.Get(GetMyKey()) as StockActorState;
        Debug.Assert(stockActorState != null);

        return MarketPlaceSerializer.SerializeStockActorState(stockActorState.stocks, stockActorState.dependencies);
    }

    public async Task<object?> ReduceStock(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var items = (Dictionary<ProductID, int>)obj;
        Debug.Assert(items.Count != 0);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var stockActorState = dictionaryState.Get(GetMyKey()) as StockActorState;
        Debug.Assert(stockActorState != null);

        var itemsReserved = new HashSet<ProductID>();
        foreach (var item in items)
        {
            if (!stockActorState.stocks.ContainsKey(item.Key)) continue;

            if (stockActorState.stocks[item.Key].quantity < item.Value) stockActorState.stocks[item.Key].quantity += item.Value * 10;
            stockActorState.stocks[item.Key].quantity -= item.Value;

            itemsReserved.Add(item.Key);
        }

        return itemsReserved;
    }

    public async Task<object?> ReplenishStock(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var productID, int quantity) = ((ProductID, int))obj;

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var stockActorState = dictionaryState.Get(GetMyKey()) as StockActorState;
        Debug.Assert(stockActorState != null);

        if (stockActorState.stocks.ContainsKey(productID)) stockActorState.stocks[productID].quantity += quantity;
        
        return null;
    }

    public async Task<object?> AddStock(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var productID, var stockInfo) = ((ProductID, StockInfo))obj;

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var stockActorState = dictionaryState.Get(GetMyKey()) as StockActorState;
        Debug.Assert(stockActorState != null);

        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalSimpleProductActor).FullName);
        
        // check if the product already exists
        if (stockActorState.stocks.ContainsKey(productID))
        {
            if (!cxt.isDet) return true;

            var call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
            await CallGrain(productActorID, call, cxt);

            return true;
        }
        else
        {
            var method = typeof(SnapperTransactionalSimpleProductActor).GetMethod("AddDependency");
            Debug.Assert(method != null);
            var call = new FunctionCall(method, new List<(ProductID, GrainID)> { (productID, myID.grainID) });

            var res = await CallGrain(productActorID, call, cxt);
            Debug.Assert(res != null);
            var info = res as Dictionary<ProductID, ProductInfo>;
            Debug.Assert(info != null);

            if (info.ContainsKey(productID))
            {
                // STEP 3: put product into cart
                stockActorState.stocks.Add(productID, stockInfo);

                // STEP 4: register the dependency info
                stockActorState.dependencies.Add(productID, productActorID);

                return true;
            }
            else return false;
        }
    }

    public async Task<object?> DeleteProduct(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var stockActorState = dictionaryState.Get(GetMyKey()) as StockActorState;
        Debug.Assert(stockActorState != null);

        stockActorState.stocks.Remove(productID);
        stockActorState.dependencies.Remove(productID);
        return null;
    }
}
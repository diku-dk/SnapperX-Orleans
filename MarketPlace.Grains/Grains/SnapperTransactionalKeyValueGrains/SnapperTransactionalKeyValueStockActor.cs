using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Common.State;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalKeyValueGrains;

public class SnapperTransactionalKeyValueStockActor : TransactionExecutionGrain, IStockActor
{
    int sellerID;

    int baseCityID;

    int cityID;

    SellerID id;

    /// <summary> Each stock actor contains the stock of products under a specific category, and those products belong to a specific seller </summary>
    public SnapperTransactionalKeyValueStockActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalKeyValueStockActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        id = new SellerID(sellerID, baseCityID);
        var info = new GeneralLongValue();
        (var dictionaryState, _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey> { id });
        (var succeed, var _) = dictionaryState.Put(id, info);
        Debug.Assert(succeed);

        return null;
    }

    public async Task<object?> ReduceStock(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var items = (Dictionary<ProductID, int>)obj;
        Debug.Assert(items.Count != 0);

        var accessModePerKey = items.Select(x => new KeyValuePair<ISnapperKey, AccessMode>(x.Key, AccessMode.ReadWrite)).ToDictionary();
        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, accessModePerKey.Keys.ToHashSet());

        var itemsReserved = new HashSet<ProductID>();
        foreach (var item in items)
        {
            var res = dictionaryState.Get(item.Key);
            if (res == null) continue;

            var stockInfo = res as StockInfo;
            if (stockInfo == null) continue;

            //if (stockInfo.quantity < item.Value) continue;
            if (stockInfo.quantity < item.Value) stockInfo.quantity += item.Value * 10;    // only for supporting PACT

            stockInfo.quantity -= item.Value;
            dictionaryState.Put(item.Key, stockInfo);

            itemsReserved.Add(item.Key);
        }
        return itemsReserved;
    }

    public async Task<object?> ReplenishStock(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var productID, int quantity) = ((ProductID, int))obj;

        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ productID });

        var res = dictionaryState.Get(productID);
        if (res == null) return null;

        var stockInfo = res as StockInfo;
        Debug.Assert(stockInfo != null);

        stockInfo.quantity += quantity;
        dictionaryState.Put(productID, stockInfo);

        return null;
    }

    public async Task<object?> AddStock(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var stock = ((ProductID, StockInfo))obj;

        var productID = stock.Item1;
        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalKeyValueProductActor).FullName);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.DeleteReference, productActorID, productID, myID.grainID, productID, new DefaultFunction());
        return await RegisterReference(cxt, referenceInfo, stock.Item2);
    }
}
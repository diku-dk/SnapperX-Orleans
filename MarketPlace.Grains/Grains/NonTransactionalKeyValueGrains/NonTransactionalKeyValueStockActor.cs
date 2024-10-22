using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Concurrency.Implementation.DataModel;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalKeyValueGrains;

public class NonTransactionalKeyValueStockActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValueStockActor
{
    int sellerID;

    int baseCityID;

    int cityID;

    /// <summary> Each stock actor contains the stock of products under a specific category, and those products belong to a specific seller </summary>
    public NonTransactionalKeyValueStockActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValueStockActor).FullName, snapperClusterCache, snapperReplicaCache) { }

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;
        return null;
    }

    public async Task<object?> ReduceStock(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        var items = (Dictionary<ProductID, int>)obj;
        Debug.Assert(items.Count != 0);

        (var dictionaryState, var _) = GetState(tid);

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

    public async Task<object?> ReplenishStock(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var productID, int quantity) = ((ProductID, int))obj;

        (var dictionaryState, var _) = GetState(tid);

        var res = dictionaryState.Get(productID);
        if (res == null) return null;

        var stockInfo = res as StockInfo;
        Debug.Assert(stockInfo != null);

        stockInfo.quantity += quantity;
        dictionaryState.Put(productID, stockInfo);

        return null;
    }

    public async Task<object?> AddStock(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        var stock = ((ProductID, StockInfo))obj;
        
        var productID = stock.Item1;
        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(NonTransactionalKeyValueProductActor).FullName);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.DeleteReference, productActorID, productID, myID.grainID, productID, new DefaultFunction());
        return await RegisterReference(tid, referenceInfo, stock.Item2);
    }
}
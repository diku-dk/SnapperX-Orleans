using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Implementation.DataModel;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalKeyValueGrains;

public class NonTransactionalKeyValueProductActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValueProductActor
{
    int sellerID;

    int baseCityID;

    /// <summary> Each product actor contains products under a specific category, and those products belong to a specific seller </summary>
    public NonTransactionalKeyValueProductActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache) 
        : base(typeof(NonTransactionalKeyValueProductActor).FullName, snapperClusterCache, snapperReplicaCache) { }

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        return null;
    }

    public async Task<object?> AddProducts(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        var products = obj as Dictionary<ProductID, ProductInfo>;
        Debug.Assert(products != null);

        (var dictionaryState, var _) = GetState(tid);
        foreach (var item in products)
        {
            var value = dictionaryState.Get(item.Key);
            if (value == null) dictionaryState.Put(item.Key, item.Value);
        }
        return null;
    }

    public async Task<object?> DeleteProduct(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = GetState(tid);
        return dictionaryState.Delete(productID);
    }

    public async Task<object?> UpdatePrice(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        (var productID, var newPrice) = ((ProductID, double))obj;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = GetState(tid);

        var res = dictionaryState.Get(productID);
        if (res == null) return null;

        var productInfo = res as ProductInfo;
        Debug.Assert(productInfo != null);
        productInfo.price = newPrice;

        dictionaryState.Put(productID, productInfo);

        return null;
    }
}
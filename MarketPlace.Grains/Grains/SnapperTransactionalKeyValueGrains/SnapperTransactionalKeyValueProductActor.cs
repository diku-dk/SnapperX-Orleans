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

public class SnapperTransactionalKeyValueProductActor : TransactionExecutionGrain, IProductActor
{
    int sellerID;

    int baseCityID;

    SellerID id;

    /// <summary> Each product actor contains products under a specific category, and those products belong to a specific seller </summary>
    public SnapperTransactionalKeyValueProductActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log) 
        : base(typeof(SnapperTransactionalKeyValueProductActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        id = new SellerID(sellerID, baseCityID);
        var info = new GeneralLongValue();
        (var dictionaryState, _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ id });
        (var succeed, var _) = dictionaryState.Put(id, info);
        Debug.Assert(succeed);
        return null;
    }

    public async Task<object?> AddProducts(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var products = obj as Dictionary<ProductID, ProductInfo>;
        Debug.Assert(products != null);

        var accessModePerKey = products.Select(x => new KeyValuePair<ISnapperKey, AccessMode>(x.Key, AccessMode.ReadWrite)).ToDictionary();
        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, accessModePerKey.Keys.ToHashSet());
        foreach (var item in products)
        {
            var value = dictionaryState.Get(item.Key);
            if (value == null) dictionaryState.Put(item.Key, item.Value);
        }
        return null;
    }

    public async Task<object?> DeleteProduct(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ productID });
        return dictionaryState.Delete(productID);
    }

    public async Task<object?> UpdatePrice(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(!cxt.isDet);

        Debug.Assert(obj != null);
        (var productID, var newPrice) = ((ProductID, double))obj;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ productID });

        var res = dictionaryState.Get(productID);
        if (res == null) return null;

        var productInfo = res as ProductInfo;
        Debug.Assert(productInfo != null);
        productInfo.price = newPrice;

        dictionaryState.Put(productID, productInfo);

        return null;
    }
}
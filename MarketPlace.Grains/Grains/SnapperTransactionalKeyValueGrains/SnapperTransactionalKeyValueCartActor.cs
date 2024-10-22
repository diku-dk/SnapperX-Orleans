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

public class SnapperTransactionalKeyValueCartActor : TransactionExecutionGrain, ICartActor
{
    int customerID;

    int baseCityID;

    CustomerID id;

    /// <summary> Each customer has a cart actor </summary>
    public SnapperTransactionalKeyValueCartActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalKeyValueCartActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        id = new CustomerID(customerID, baseCityID);
        var info = new GeneralLongValue(0);
        (var dictionaryState, _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey> { id });
        (var succeed, var _) = dictionaryState.Put(id, info);
        Debug.Assert(succeed);
        return null;
    }

    public async Task<object?> DeleteItemInCart(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalKeyValueProductActor).FullName);

        // STEP 0: check if the item is in the cart
        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ productID });
        (var succeed, var msg) = dictionaryState.Delete(productID);
        if (!succeed && cxt.isDet)
        {
            var call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
            await CallGrain(productActorID, call, cxt);
        }

        return null;
    }

    /// <returns> product info </returns>
    public async Task<object?> AddItemToCart(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalKeyValueProductActor).FullName);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.ReplicateReference, productActorID, productID, myID.grainID, productID, new DefaultUpdateFunction());

        return await RegisterReference(cxt, referenceInfo);
    }

    public async Task<object?> Checkout(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        // STEP 0: find the items that actually exists in the cart
        var accessModePerKey = checkout.items.Select(x => new KeyValuePair<ISnapperKey, AccessMode>(x.Key, AccessMode.ReadWrite)).ToDictionary();
        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, accessModePerKey.Keys.ToHashSet());
        var infoPerProduct = checkout.infoPerProduct;
        foreach (var item in checkout.items)
        {
            var info = dictionaryState.Get(item.Key);
            if (info != null)
            {
                var productInfo = info as ProductInfo;
                Debug.Assert(productInfo != null);
                infoPerProduct.Add(item.Key, productInfo);
            }
        }

        // STEP 1: forward to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(customerID, baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(SnapperTransactionalKeyValueOrderActor).FullName);

        var method = typeof(SnapperTransactionalKeyValueOrderActor).GetMethod("NewOrder");
        Debug.Assert(method != null);
        var call = new FunctionCall(method, checkout);
        var res = await CallGrain(orderActorID, call, cxt);

        Debug.Assert(res != null);
        var orderItemsPerSeller = res as Dictionary<SellerID, List<OrderItem>>;
        Debug.Assert(orderItemsPerSeller != null);

        // STEP 2: remove the bought items from cart
        var accessedProductActorIDs = new HashSet<GrainID>();
        foreach (var items in orderItemsPerSeller)
        {
            foreach (var item in items.Value)
            {
                var productID = item.productID;
                dictionaryState.Delete(productID);    // this deletion will cause the de-registration of the dependency on the product actor
                Debug.Assert(infoPerProduct.ContainsKey(productID));

                var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
                var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalKeyValueProductActor).FullName);
                accessedProductActorIDs.Add(productActorID);
            }
        }

        if (!cxt.isDet) return null;

        // STEP 3: fulfill the call to product actors for items that do not exist in the cart
        var productActorIDs = new HashSet<GrainID>();
        foreach (var item in checkout.items)
        {
            var productID = item.Key;
            var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
            var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalKeyValueProductActor).FullName);

            if (!accessedProductActorIDs.Contains(productActorID)) productActorIDs.Add(productActorID);
        }

        var tasks1 = new List<Task>();
        call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
        foreach (var productActorID in productActorIDs) tasks1.Add(CallGrain(productActorID, call, cxt));
        await Task.WhenAll(tasks1);

        return null;
    }
}
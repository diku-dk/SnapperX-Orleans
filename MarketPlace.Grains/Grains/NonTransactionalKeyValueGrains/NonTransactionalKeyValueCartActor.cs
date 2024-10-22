using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Concurrency.Implementation.DataModel;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using System.Diagnostics;
using System.Reflection;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalKeyValueGrains;

public class NonTransactionalKeyValueCartActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValueCartActor
{
    int customerID;

    int baseCityID;

    /// <summary> Each customer has a cart actor </summary>
    public NonTransactionalKeyValueCartActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValueCartActor).FullName, snapperClusterCache, snapperReplicaCache) { }

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        return null;
    }

    public async Task<object?> DeleteItemInCart(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(NonTransactionalKeyValueProductActor).FullName);

        // STEP 0: check if the item is in the cart
        (var dictionaryState, var _) = GetState(tid);
        (var succeed, var reason) = dictionaryState.Delete(productID);
        
        return null;
    }

    /// <returns> product info </returns>
    public async Task<object?> AddItemToCart(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(NonTransactionalKeyValueProductActor).FullName);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.ReplicateReference, productActorID, productID, myID.grainID, productID, new DefaultUpdateFunction());

        return await RegisterReference(tid, referenceInfo);
    }

    public async Task<object?> Checkout(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        bool succeed;
        string msg;
        MethodInfo? method;
        FunctionCall? call;
        object? res;

        // STEP 0: find the items that actually exists in the cart
        (var dictionaryState, var _) = GetState(tid);
        var infoPerProduct = new Dictionary<ProductID, ProductInfo>();
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

        // STEP 1: resolve the set of items to buy from different sellers
        var itemsPerSeller = new Dictionary<SellerID, Dictionary<ProductID, int>>();   // seller ID, product ID, quantity
        foreach (var item in checkout.items)
        {
            var sellerID = item.Key.sellerID;
            if (!itemsPerSeller.ContainsKey(sellerID)) itemsPerSeller.Add(sellerID, new Dictionary<ProductID, int>());

            // the item that does not exist in the cart will be bought with quantity = 0
            var quantity = item.Value;
            if (!infoPerProduct.ContainsKey(item.Key)) quantity = 0;

            itemsPerSeller[sellerID].Add(item.Key, quantity);
        }
        
        // STEP 2: reduce stock of target items
        var tasks = new Dictionary<SellerID, Task<object?>>();
        var deliveryCityID = checkout.deliveryAddress.cityID;  // only check the stock actors in the delivery city
        foreach (var item in itemsPerSeller)
        {
            var sellerID = item.Key;
            var stockActorGuid = IdMapping.GetStockActorGuid(sellerID.id, deliveryCityID);
            var stockActorID = new GrainID(stockActorGuid, typeof(NonTransactionalKeyValueStockActor).FullName);

            method = typeof(NonTransactionalKeyValueStockActor).GetMethod("ReduceStock");
            Debug.Assert(method != null);
            call = new FunctionCall(method, item.Value);
            tasks.Add(sellerID, CallGrain(stockActorID, call, tid));
        }
        await Task.WhenAll(tasks.Values);

        // STEP 3: resolve the items that actually bought
        double moneyToPay = 0;
        var orderItemsPerSeller = new Dictionary<SellerID, List<OrderItem>>();
        foreach (var task in tasks)
        {
            var items = task.Value.Result as HashSet<ProductID>;
            Debug.Assert(items != null);

            if (items.Count == 0) continue;

            var sellerID = task.Key;
            orderItemsPerSeller.Add(sellerID, new List<OrderItem>());
            foreach (var item in items)
            {
                if (!infoPerProduct.ContainsKey(item)) continue;

                var quantity = itemsPerSeller[sellerID][item];
                var productInfo = infoPerProduct[item];
                orderItemsPerSeller[sellerID].Add(new OrderItem(item, productInfo, quantity, deliveryCityID));
                moneyToPay += quantity * productInfo.price;
            }
        }

        // STEP 4: process payment
        var paymentActorGuid = IdMapping.GetPaymentActorGuid(customerID, baseCityID);
        var paymentActorID = new GrainID(paymentActorGuid, typeof(NonTransactionalKeyValuePaymentActor).FullName);
        
        method = typeof(NonTransactionalKeyValuePaymentActor).GetMethod("ProcessPayment");
        Debug.Assert(method != null);
        call = new FunctionCall(method, (checkout.paymentMethod, moneyToPay));
        res = await CallGrain(paymentActorID, call, tid);

        Debug.Assert(res != null);
        (succeed, msg) = ((bool, string))res;
        Debug.Assert(succeed);

        // STEP 5: forward to the order actor to continue
        var orderActorGuid = IdMapping.GetOrderActorGuid(customerID, baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(NonTransactionalKeyValueOrderActor).FullName);
        
        method = typeof(NonTransactionalKeyValueOrderActor).GetMethod("NewOrder");
        Debug.Assert(method != null);
        var paymentInfo = new PaymentInfo(new CustomerID(customerID, baseCityID), DateTime.UtcNow, checkout.paymentMethod, msg);
        
        call = new FunctionCall(method, (paymentInfo, orderItemsPerSeller, deliveryCityID));
        await CallGrain(orderActorID, call, tid);
        
        // STEP 6: remove the bought items from cart
        var accessedProductActorIDs = new HashSet<GrainID>();
        foreach (var items in orderItemsPerSeller)
        {
            foreach (var item in items.Value)
            {
                var productID = item.productID;
                dictionaryState.Delete(productID);    // this deletion will cause the de-registration of the dependency on the product actor
                Debug.Assert(infoPerProduct.ContainsKey(productID));

                var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
                var productActorID = new GrainID(productActorGuid, typeof(NonTransactionalKeyValueProductActor).FullName);
                accessedProductActorIDs.Add(productActorID);
            }
        }

        return null;
    }
}
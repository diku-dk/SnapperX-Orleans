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

public class SnapperTransactionalSimpleCartActor : TransactionExecutionGrain, ICartActor
{
    int customerID;

    int baseCityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleCartActor");
        return myKey;
    }

    /// <summary> Each customer has a cart actor </summary>
    public SnapperTransactionalSimpleCartActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleCartActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var cartActorState = new CartActorState(new Dictionary<ProductID, ProductInfo>(), new Dictionary<ProductID, GrainID>());
        dictionaryState.Put(GetMyKey(), cartActorState);

        return null;
    }

    public async Task<object?> ReadSimpleState(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.Read, cxt);
        var cartActorState = dictionaryState.Get(GetMyKey()) as CartActorState;
        Debug.Assert(cartActorState != null);

        return MarketPlaceSerializer.SerializeCartActorState(cartActorState.cartItems, cartActorState.dependencies);
    }

    public async Task<object?> DeleteItemInCart(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // STEP 1: check if the item is in the cart
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var cartActorState = dictionaryState.Get(GetMyKey()) as CartActorState;
        Debug.Assert(cartActorState != null);

        var succeed = cartActorState.cartItems.Remove(productID);
        if (!succeed && !cxt.isDet) return null;

        // STEP 2: delete the dependency info on the current actor
        GrainID? productActorID = null;
        if (cartActorState.dependencies.ContainsKey(productID))
        {
            productActorID = cartActorState.dependencies[productID];
            cartActorState.dependencies.Remove(productID);
        }
       
        // STEP 3: delete the dependency info on the origin actor
        
        if (succeed && productActorID != null)
        {
            var method = typeof(SnapperTransactionalSimpleProductActor).GetMethod("RemoveDependencies");
            Debug.Assert(method != null);
            var call = new FunctionCall(method, new List<(ProductID, GrainID)> { (productID, myID.grainID) });

            await CallGrain(productActorID, call, cxt);
        }
        else
        {
            var calculatedProductActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
            var calculatedProductActorID = new GrainID(calculatedProductActorGuid, typeof(SnapperTransactionalSimpleProductActor).FullName);

            var call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
            await CallGrain(calculatedProductActorID, call, cxt);
        }

        return null;
    }

    /// <returns> product info </returns>
    public async Task<object?> AddItemToCart(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // STEP 1: check if the product already exist
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var cartActorState = dictionaryState.Get(GetMyKey()) as CartActorState;
        Debug.Assert(cartActorState != null);

        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalSimpleProductActor).FullName);

        if (!cartActorState.cartItems.ContainsKey(productID) && !cartActorState.dependencies.ContainsKey(productID))
        {
            // STEP 2: register the dependency on the product actor
            var method = typeof(SnapperTransactionalSimpleProductActor).GetMethod("AddDependency");
            Debug.Assert(method != null);
            var call = new FunctionCall(method, new List<(ProductID, GrainID)>{ (productID, myID.grainID) });

            var res = await CallGrain(productActorID, call, cxt);
            Debug.Assert(res != null);
            var info = res as Dictionary<ProductID, ProductInfo>;
            Debug.Assert(info != null);

            if (info.ContainsKey(productID))
            {
                var productInfo = info[productID];

                // STEP 3: put product into cart
                cartActorState.cartItems.Add(productID, productInfo);

                // STEP 4: register the dependency info
                cartActorState.dependencies.Add(productID, productActorID);
            }
        }
        else
        {
            if (cxt.isDet)
            {
                var call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
                await CallGrain(productActorID, call, cxt);
            }
        }

        return null;
    }

    public async Task<object?> UpdateOrDeleteProduct(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var productID, var productInfo) = ((ProductID, ProductInfo?))obj;
        Debug.Assert(productID != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var cartActorState = dictionaryState.Get(GetMyKey()) as CartActorState;
        Debug.Assert(cartActorState != null);

        if (productInfo == null)
        {
            // the product is deleted

            // remove the item from cart
            cartActorState.cartItems.Remove(productID);

            // remove the registered dependency
            cartActorState.dependencies.Remove(productID);
        }
        else
        {
            // the product is updated
            if (cartActorState.cartItems.ContainsKey(productID)) cartActorState.cartItems[productID] = productInfo;
        }

        return null;
    }

    public async Task<object?> Checkout(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var cartActorState = dictionaryState.Get(GetMyKey()) as CartActorState;
        Debug.Assert(cartActorState != null);
        
        // STEP 0: find the items that actually exists in the cart
        var infoPerProduct = checkout.infoPerProduct;
        foreach (var item in checkout.items)
        {
            if (cartActorState.cartItems.ContainsKey(item.Key))
                infoPerProduct.Add(item.Key, cartActorState.cartItems[item.Key]);
        }

        // STEP 1: forward to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(customerID, baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(SnapperTransactionalSimpleOrderActor).FullName);

        var method = typeof(SnapperTransactionalSimpleOrderActor).GetMethod("NewOrder");
        Debug.Assert(method != null);
        var call = new FunctionCall(method, checkout);
        var res = await CallGrain(orderActorID, call, cxt);
        
        Debug.Assert(res != null);
        var orderItemsPerSeller = res as Dictionary<SellerID, List<OrderItem>>;
        Debug.Assert(orderItemsPerSeller != null);

        // STEP 2: remove the bought items from cart
        var itemsToRemovePerProductActor = new Dictionary<GrainID, List<(ProductID, GrainID)>>();
        foreach (var items in orderItemsPerSeller)
        {
            foreach (var item in items.Value)
            {
                var productID = item.productID;
                Debug.Assert(cartActorState.dependencies.ContainsKey(productID));
                var productActorID = cartActorState.dependencies[productID];
                cartActorState.cartItems.Remove(productID);
                cartActorState.dependencies.Remove(productID);

                if (!itemsToRemovePerProductActor.ContainsKey(productActorID)) itemsToRemovePerProductActor.Add(productActorID, new List<(ProductID, GrainID)>());
                itemsToRemovePerProductActor[productActorID].Add((productID, myID.grainID));
            }
        }

        // STEP 3: de-register the dependency on the product actor
        var newTasks = new List<Task>();
        foreach (var item in itemsToRemovePerProductActor)
        {
            var productActorID = item.Key;

            method = typeof(SnapperTransactionalSimpleProductActor).GetMethod("RemoveDependencies");
            Debug.Assert(method != null);
            call = new FunctionCall(method, item.Value);
            newTasks.Add(CallGrain(item.Key, call, cxt));
        }

        if (!cxt.isDet)
        {
            await Task.WhenAll(newTasks);
            return null;
        }

        // STEP 4: fulfill the call to product actors for items that do not exist in the cart
        var otherProductActors = new HashSet<GrainID>();
        foreach (var item in checkout.items)
        {
            var productID = item.Key;
            var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
            var productActorID = new GrainID(productActorGuid, typeof(SnapperTransactionalSimpleProductActor).FullName);
            if (itemsToRemovePerProductActor.ContainsKey(productActorID)) continue;

            if (otherProductActors.Contains(productActorID)) continue;

            call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
            newTasks.Add(CallGrain(productActorID, call, cxt));
            otherProductActors.Add(productActorID);
        }
        await Task.WhenAll(newTasks);

        return null;
    }
}
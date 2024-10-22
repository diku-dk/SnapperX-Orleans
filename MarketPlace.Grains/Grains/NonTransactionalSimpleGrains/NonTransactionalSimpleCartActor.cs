using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalSimpleGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class NonTransactionalSimpleCartActor : Grain, INonTransactionalSimpleCartActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int customerID;
    int baseCityID;

    Dictionary<ProductID, ProductInfo> cartItems;
    Dictionary<ProductID, GrainID> dependencies;

    public NonTransactionalSimpleCartActor(ISnapperClusterCache snapperClusterCache) => this.snapperClusterCache = snapperClusterCache;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(NonTransactionalSimpleCartActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        cartItems = new Dictionary<ProductID, ProductInfo>();
        dependencies = new Dictionary<ProductID, GrainID>();

        return Task.FromResult(new TransactionResult());
    }

    public Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        txnResult.SetResult(MarketPlaceSerializer.SerializeCartActorState(cartItems, dependencies));
        return Task.FromResult(txnResult);
    }

    public async Task<TransactionResult> DeleteItemInCart(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // STEP 1: check if the item is in the cart
        var succeed = cartItems.Remove(productID);
        txnResult.AddGrain(myID);

        if (!succeed) return txnResult;

        // STEP 2: delete the dependency info on the current actor
        GrainID? productActorID = null;
        if (dependencies.ContainsKey(productID))
        {
            productActorID = dependencies[productID];
            dependencies.Remove(productID);
        }

        // STEP 3: delete the dependency info on the origin actor
        if (productActorID != null)
        {
            var productActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleProductActor>(snapperClusterCache, GrainFactory, myRegionID, productActorID);
            var res = await productActor.RemoveDependency((productID, myID.grainID));

            txnResult.MergeResult(res);
        }

        return txnResult;
    }

    public async Task<TransactionResult> AddItemToCart(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // STEP 1: check if the product already exist
        if (cartItems.ContainsKey(productID) || dependencies.ContainsKey(productID))
        {
            txnResult.AddGrain(myID);
            return txnResult;
        }

        // STEP 2: register the dependency on the product actor
        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(NonTransactionalSimpleProductActor).FullName);

        var productActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleProductActor>(snapperClusterCache, GrainFactory, myRegionID, productActorID);
        var res = await productActor.AddDependency((productID, myID.grainID));
        txnResult.MergeResult(res);

        if (res.resultObj == null) return txnResult;

        // STEP 3: put product into cart
        var productInfo = res.resultObj as ProductInfo;
        Debug.Assert(productInfo != null);
        cartItems.TryAdd(productID, productInfo);

        // STEP 4: register the dependency info
        dependencies.TryAdd(productID, productActorID);

        txnResult.AddGrain(myID);
        return txnResult;
    }

    public Task<TransactionResult> UpdateOrDeleteProduct(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var productInfo) = ((ProductID, ProductInfo?))obj;
        Debug.Assert(productID != null);

        if (productInfo == null)
        {
            // the product is deleted

            // remove the item from cart
            cartItems.Remove(productID);

            // remove the registered dependency
            dependencies.Remove(productID);
        }
        else
        {
            // the product is updated
            if (cartItems.ContainsKey(productID)) cartItems[productID] = productInfo;
        }

        txnResult.AddGrain(myID);
        return Task.FromResult(txnResult);
    }

    public async Task<TransactionResult> Checkout(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        // STEP 0: find the items that actually exists in the cart
        var infoPerProduct = new Dictionary<ProductID, ProductInfo>();
        foreach (var item in checkout.items) if (cartItems.ContainsKey(item.Key)) infoPerProduct.Add(item.Key, cartItems[item.Key]);
        txnResult.AddGrain(myID);

        // STEP 1: forward to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(customerID, baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(NonTransactionalSimpleOrderActor).FullName);
        var orderActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleOrderActor>(snapperClusterCache, GrainFactory, myRegionID, orderActorID);
        var res = await orderActor.NewOrder(checkout);
        txnResult.MergeResult(res);

        Debug.Assert(res.resultObj != null);
        var orderItemsPerSeller = res.resultObj as Dictionary<SellerID, List<OrderItem>>;
        Debug.Assert(orderItemsPerSeller != null);

        // STEP 6: remove the bought items from cart
        var newTasks = new List<Task<TransactionResult>>();
        foreach (var items in orderItemsPerSeller)
        {
            foreach (var item in items.Value)
            {
                var productID = item.productID;
                newTasks.Add(DeleteItemInCart(productID));
            }
        }
        await Task.WhenAll(newTasks);

        foreach (var task in newTasks) txnResult.MergeResult(task.Result);

        return txnResult;
    }
}
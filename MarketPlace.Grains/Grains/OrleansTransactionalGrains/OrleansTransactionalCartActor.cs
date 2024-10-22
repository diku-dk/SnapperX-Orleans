using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Orleans.Transactions.Abstractions;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.OrleansTransactionalGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalCartActor : Grain, IOrleansTransactionalCartActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int customerID;
    int baseCityID;

    readonly ITransactionalState<Dictionary<ProductID, ProductInfo>> cartItems;
    readonly ITransactionalState<Dictionary<ProductID, GrainID>> dependencies;

    public OrleansTransactionalCartActor(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(cartItems))] ITransactionalState<Dictionary<ProductID, ProductInfo>> cartItems,
        [TransactionalState(nameof(dependencies))] ITransactionalState<Dictionary<ProductID, GrainID>> dependencies)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.cartItems = cartItems ?? throw new ArgumentException(nameof(cartItems));
        this.dependencies = dependencies ?? throw new ArgumentException(nameof(dependencies));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalCartActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        await cartItems.PerformUpdate(x => x = new Dictionary<ProductID, ProductInfo>());
        await dependencies.PerformUpdate(x => x = new Dictionary<ProductID, GrainID>());

        return new TransactionResult();
    }

    public async Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        var item1 = await cartItems.PerformRead(x => x);
        var item2 = await dependencies.PerformRead(x => x);
        txnResult.SetResult(MarketPlaceSerializer.SerializeCartActorState(item1, item2));
        return txnResult;
    }

    public async Task<TransactionResult> DeleteItemInCart(object? obj = null)
    {
        var txnResult = new TransactionResult();
        
        Debug.Assert(obj != null);
        var productID = obj as ProductID;
        Debug.Assert(productID != null);

        // STEP 1: check if the item is in the cart
        var succeed = await cartItems.PerformUpdate(x => x.Remove(productID));
        txnResult.AddGrain(myID);

        if (!succeed) return txnResult;

        // STEP 2: delete the dependency info on the current actor
        var productActorID = await dependencies.PerformUpdate(x =>
        {
            if (!x.ContainsKey(productID)) return null;

            var info = x[productID];
            x.Remove(productID);
            return info;
        });

        // STEP 3: delete the dependency info on the origin actor
        if (productActorID != null)
        {
            var productActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalProductActor>(snapperClusterCache, GrainFactory, myRegionID, productActorID);
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
        var productExists = await cartItems.PerformRead(x => x.ContainsKey(productID));
        var referenceExists = await dependencies.PerformRead(x => x.ContainsKey(productID));
        if (productExists || referenceExists)
        {
            txnResult.AddGrain(myID);
            return txnResult;
        }

        // STEP 2: register the dependency on the product actor
        var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
        var productActorID = new GrainID(productActorGuid, typeof(OrleansTransactionalProductActor).FullName);

        var productActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalProductActor>(snapperClusterCache, GrainFactory, myRegionID, productActorID);
        var res = await productActor.AddDependency((productID, myID.grainID));
        txnResult.MergeResult(res);

        if (res.resultObj == null) return txnResult;

        // STEP 3: put product into cart
        var productInfo = res.resultObj as ProductInfo;
        Debug.Assert(productInfo != null);
        await cartItems.PerformUpdate(x => x.Add(productID, productInfo));

        // STEP 4: register the dependency info
        await dependencies.PerformUpdate(x => x.Add(productID, productActorID));

        txnResult.AddGrain(myID);
        return txnResult;
    }

    /// <summary> the method is called by the product actor when it deletes or updates a product </summary>
    public async Task<TransactionResult> UpdateOrDeleteProduct(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var productID, var productInfo) = ((ProductID, ProductInfo?))obj;
        Debug.Assert(productID != null);

        if (productInfo == null)
        {
            // the product is deleted

            // remove the item from cart
            await cartItems.PerformUpdate(x => x.Remove(productID));

            // remove the registered dependency
            await dependencies.PerformUpdate(x => x.Remove(productID));
        }
        else
        {
            // the product is updated
            await cartItems.PerformUpdate(x => { if (x.ContainsKey(productID)) x[productID] = productInfo; });
        }

        txnResult.AddGrain(myID);
        return txnResult;
    }

    public async Task<TransactionResult> Checkout(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        // STEP 0: find the items that actually exists in the cart
        var infoPerProduct = checkout.infoPerProduct;
        foreach (var item in checkout.items)
        {
            var info = await cartItems.PerformRead(x => 
            {
                if (x.ContainsKey(item.Key)) return x[item.Key];
                else return null;
            });

            if (info != null)
            {
                var productInfo = info as ProductInfo;
                Debug.Assert(productInfo != null);
                infoPerProduct.Add(item.Key, productInfo);
            }
        }
        txnResult.AddGrain(myID);

        // STEP 1: forward to an order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(customerID, baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(OrleansTransactionalOrderActor).FullName);
        var orderActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalOrderActor>(snapperClusterCache, GrainFactory, myRegionID, orderActorID);
        var res = await orderActor.NewOrder(checkout);
        txnResult.MergeResult(res);

        Debug.Assert(res.resultObj != null);
        var orderItemsPerSeller = res.resultObj as Dictionary<SellerID, List<OrderItem>>;
        Debug.Assert(orderItemsPerSeller != null);

        // STEP 2: remove the bought items from cart
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
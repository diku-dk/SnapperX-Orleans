using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using Orleans.Transactions.Abstractions;
using System.Diagnostics;

namespace MarketPlace.Grains.OrleansTransactionalGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalOrderActor : Grain, IOrleansTransactionalOrderActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int customerID;
    int baseCityID;

    readonly ITransactionalState<GeneralLongValue> nextOrderID;
    readonly ITransactionalState<Dictionary<PackageID, PackageInfo>> packageInfo;
    readonly ITransactionalState<Dictionary<PackageID, GrainID>> dependencies;

    readonly ITransactionalState<Dictionary<OrderID, OrderInfo>> orderInfo;

    public OrleansTransactionalOrderActor(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(nextOrderID))] ITransactionalState<GeneralLongValue> nextOrderID,
        [TransactionalState(nameof(packageInfo))] ITransactionalState<Dictionary<PackageID, PackageInfo>> packageInfo,
        [TransactionalState(nameof(dependencies))] ITransactionalState<Dictionary<PackageID, GrainID>> dependencies,
        [TransactionalState(nameof(orderInfo))] ITransactionalState<Dictionary<OrderID, OrderInfo>> orderInfo)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.nextOrderID = nextOrderID ?? throw new ArgumentException(nameof(nextOrderID));
        this.packageInfo = packageInfo ?? throw new ArgumentException(nameof(packageInfo));
        this.dependencies = dependencies ?? throw new ArgumentException(nameof(dependencies));
        this.orderInfo = orderInfo ?? throw new ArgumentException(nameof(orderInfo));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalOrderActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        await nextOrderID.PerformUpdate(x => x = new GeneralLongValue(0));
        await packageInfo.PerformUpdate(x => x = new Dictionary<PackageID, PackageInfo>());
        await dependencies.PerformUpdate(x => x = new Dictionary<PackageID, GrainID>());
        await orderInfo.PerformUpdate(x => x = new Dictionary<OrderID, OrderInfo>());

        return new TransactionResult();
    }

    public async Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        var item1 = await nextOrderID.PerformRead(x => x);
        var item2 = await packageInfo.PerformRead(x => x);
        var item3 = await dependencies.PerformRead(x => x);
        var item4 = await orderInfo.PerformRead(x => x);
        txnResult.SetResult(MarketPlaceSerializer.SerializeOrderActorState(item1, item2, item3, item4));
        return txnResult;
    }

    public async Task<TransactionResult> NewOrder(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        // STEP 1: resolve the set of items to buy from different sellers
        var itemsPerSeller = new Dictionary<SellerID, Dictionary<ProductID, int>>();   // seller ID, product ID, quantity
        foreach (var item in checkout.items)
        {
            var sellerID = item.Key.sellerID;
            if (!itemsPerSeller.ContainsKey(sellerID)) itemsPerSeller.Add(sellerID, new Dictionary<ProductID, int>());

            var quantity = item.Value;
            if (!checkout.infoPerProduct.ContainsKey(item.Key)) quantity = 0;

            itemsPerSeller[sellerID].Add(item.Key, quantity);
        }

        // STEP 2: reduce stock of target items
        var tasks = new Dictionary<SellerID, Task<TransactionResult>>();
        var deliveryCityID = checkout.deliveryAddress.cityID;  // only check the stock actors in the delivery city
        foreach (var item in itemsPerSeller)
        {
            var sellerID = item.Key;
            var stockActorGuid = IdMapping.GetStockActorGuid(sellerID.id, deliveryCityID);
            var stockActorID = new GrainID(stockActorGuid, typeof(OrleansTransactionalStockActor).FullName);
            var stockActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalStockActor>(snapperClusterCache, GrainFactory, myRegionID, stockActorID);

            tasks.Add(sellerID, stockActor.ReduceStock(item.Value));
        }
        await Task.WhenAll(tasks.Values);

        // STEP 3: resolve the items that actually bought
        double moneyToPay = 0;
        var orderItemsPerSeller = new Dictionary<SellerID, List<OrderItem>>();
        foreach (var task in tasks)
        {
            txnResult.MergeResult(task.Value.Result);

            Debug.Assert(task.Value.Result.resultObj != null);
            var items = task.Value.Result.resultObj as HashSet<ProductID>;
            Debug.Assert(items != null);

            if (items.Count == 0) continue;

            var sellerID = task.Key;
            orderItemsPerSeller.Add(sellerID, new List<OrderItem>());
            foreach (var item in items)
            {
                if (!checkout.infoPerProduct.ContainsKey(item)) continue;

                var quantity = itemsPerSeller[sellerID][item];
                var productInfo = checkout.infoPerProduct[item];
                orderItemsPerSeller[sellerID].Add(new OrderItem(item, productInfo, quantity, deliveryCityID));
                moneyToPay += quantity * productInfo.price;
            }
        }

        // STEP 4: process payment
        var paymentActorGuid = IdMapping.GetPaymentActorGuid(customerID, baseCityID);
        var paymentActorID = new GrainID(paymentActorGuid, typeof(OrleansTransactionalPaymentActor).FullName);
        var paymentActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalPaymentActor>(snapperClusterCache, GrainFactory, myRegionID, paymentActorID);
        var res = await paymentActor.ProcessPayment((checkout.paymentMethod, moneyToPay));
        txnResult.MergeResult(res);

        Debug.Assert(res.resultObj != null);
        (var succeed, var msg) = ((bool, string))res.resultObj;
        Debug.Assert(succeed);

        // STEP 5: forward to the order actor to continue
        var paymentInfo = new PaymentInfo(new CustomerID(customerID, baseCityID), DateTime.UtcNow, checkout.paymentMethod, msg);
        res = await CreateOrder(paymentInfo, orderItemsPerSeller, deliveryCityID);
        txnResult.MergeResult(res);

        txnResult.SetResult(orderItemsPerSeller);

        return txnResult;
    }

    async Task<TransactionResult> CreateOrder(PaymentInfo paymentInfo, Dictionary<SellerID, List<OrderItem>> orderItemsPerSeller, int deliveryCityID)
    {
        var txnResult = new TransactionResult();

        // STEP 1: get an order ID, and update nextOrderID
        var id = await nextOrderID.PerformUpdate(x => { var i = x.value; x.value++; return i; });
        txnResult.AddGrain(myID);

        var orderID = new OrderID(new CustomerID(customerID, baseCityID), id);

        // STEP 2: forward to a shipment actor to create a package for each seller
        var allOrderItems = new List<OrderItem>();
        var tasks = new Dictionary<GrainID, Task<TransactionResult>>();
        foreach (var item in orderItemsPerSeller)
        {
            var sellerID = item.Key;

            var shipmentActorGuid = IdMapping.GetShipmentActorGuid(sellerID.id, deliveryCityID);
            var shipmentActorID = new GrainID(shipmentActorGuid, typeof(OrleansTransactionalShipmentActor).FullName);
            var shipmentActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalShipmentActor>(snapperClusterCache, GrainFactory, myRegionID, shipmentActorID);
            tasks.Add(shipmentActorID, shipmentActor.NewPackage((orderID, item.Value, deliveryCityID)));

            allOrderItems.AddRange(item.Value);
        }
        await Task.WhenAll(tasks.Values);

        // STEP 3: add package info to dictionary
        var packages = new List<PackageID>();
        foreach (var t in tasks)
        {
            var shipmentActorID = t.Key;

            txnResult.MergeResult(t.Value.Result);

            Debug.Assert(t.Value.Result.resultObj != null);
            (var packageID, var packageInfo) = ((PackageID, PackageInfo))t.Value.Result.resultObj;
            Debug.Assert(packageID != null && packageInfo != null);

            packages.Add(packageID);

            // whenever the package is updated by the shipment actor, the update will forward to this order actor as well
            //await this.packageInfo.PerformUpdate(x => x.Add(packageID, packageInfo));
            //await dependencies.PerformUpdate(x => x.Add(packageID, shipmentActorID));
        }

        // STEP 4: add order info to dictionary
        var orderInfo = new OrderInfo(paymentInfo, allOrderItems, packages);
        //await this.orderInfo.PerformUpdate(x => x.Add(orderID, orderInfo));

        return txnResult;
    }
}
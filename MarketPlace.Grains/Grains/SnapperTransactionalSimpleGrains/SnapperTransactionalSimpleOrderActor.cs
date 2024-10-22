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
using System.Reflection;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains;

public class SnapperTransactionalSimpleOrderActor : TransactionExecutionGrain, IOrderActor
{
    int customerID;

    int baseCityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleOrderActor");
        return myKey;
    }

    public SnapperTransactionalSimpleOrderActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleOrderActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var orderActorState = new OrderActorState(new GeneralLongValue(0), new Dictionary<PackageID, PackageInfo>(), new Dictionary<PackageID, GrainID>(), new Dictionary<OrderID, OrderInfo>());
        dictionaryState.Put(GetMyKey(), orderActorState);
        return null;
    }

    public async Task<object?> ReadSimpleState(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.Read, cxt);
        var orderActorState = dictionaryState.Get(GetMyKey()) as OrderActorState;
        Debug.Assert(orderActorState != null);

        return MarketPlaceSerializer.SerializeOrderActorState(orderActorState.nextOrderID, orderActorState.packageInfo, orderActorState.dependencies, orderActorState.orderInfo);
    }

    public async Task<object?> NewOrder(TransactionContext cxt, object? obj = null)
    {
        bool succeed;
        string msg;
        MethodInfo? method;
        FunctionCall? call;
        object? res;

        Debug.Assert(obj != null);
        var checkout = obj as Checkout;
        Debug.Assert(checkout != null);

        // STEP 1: resolve the set of items to buy from different sellers
        var itemsPerSeller = new Dictionary<SellerID, Dictionary<ProductID, int>>();   // seller ID, product ID, quantity
        foreach (var item in checkout.items)
        {
            var sellerID = item.Key.sellerID;
            if (!itemsPerSeller.ContainsKey(sellerID)) itemsPerSeller.Add(sellerID, new Dictionary<ProductID, int>());

            // the item that does not exist in the cart will be bought with quantity = 0
            var quantity = item.Value;
            if (!checkout.infoPerProduct.ContainsKey(item.Key)) quantity = 0;
            itemsPerSeller[sellerID].Add(item.Key, quantity);
        }

        // STEP 2: reduce stock of target items
        var tasks = new Dictionary<SellerID, Task<object?>>();
        var deliveryCityID = checkout.deliveryAddress.cityID;  // only check the stock actors in the delivery city
        foreach (var item in itemsPerSeller)
        {
            var sellerID = item.Key;
            var stockActorGuid = IdMapping.GetStockActorGuid(sellerID.id, deliveryCityID);
            var stockActorID = new GrainID(stockActorGuid, typeof(SnapperTransactionalSimpleStockActor).FullName);

            method = typeof(SnapperTransactionalSimpleStockActor).GetMethod("ReduceStock");
            Debug.Assert(method != null);
            call = new FunctionCall(method, item.Value);
            tasks.Add(sellerID, CallGrain(stockActorID, call, cxt));
        }
        await Task.WhenAll(tasks.Values);

        // STEP 3: resolve the items that actually bought
        double moneyToPay = 0;
        var orderItemsPerSeller = new Dictionary<SellerID, List<OrderItem>>();
        foreach (var task in tasks)
        {
            var items = task.Value.Result as HashSet<ProductID>;
            Debug.Assert(items != null);

            if (!cxt.isDet && items.Count == 0) continue;

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
        var paymentActorID = new GrainID(paymentActorGuid, typeof(SnapperTransactionalSimplePaymentActor).FullName);

        method = typeof(SnapperTransactionalSimplePaymentActor).GetMethod("ProcessPayment");
        Debug.Assert(method != null);
        call = new FunctionCall(method, (checkout.paymentMethod, moneyToPay));
        res = await CallGrain(paymentActorID, call, cxt);
        Debug.Assert(res != null);
        (succeed, msg) = ((bool, string))res;
        Debug.Assert(succeed);

        // STEP 5: forward to the order actor to continue
        var paymentInfo = new PaymentInfo(new CustomerID(customerID, baseCityID), DateTime.UtcNow, checkout.paymentMethod, msg);

        await CreateOrder(cxt, paymentInfo, orderItemsPerSeller, deliveryCityID);

        return orderItemsPerSeller;
    }

    async Task CreateOrder(TransactionContext cxt, PaymentInfo paymentInfo, Dictionary<SellerID, List<OrderItem>> orderItemsPerSeller, int deliveryCityID)
    {
        // STEP 1: get an order ID, and update nextOrderID
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var orderActorState = dictionaryState.Get(GetMyKey()) as OrderActorState;
        Debug.Assert(orderActorState != null);

        var id = orderActorState.nextOrderID.value;
        var orderID = new OrderID(new CustomerID(customerID, baseCityID), id);
        orderActorState.nextOrderID.value++;

        // STEP 2: forward to a shipment actor to create a package for each seller
        var allOrderItems = new List<OrderItem>();
        var tasks = new Dictionary<GrainID, Task<object?>>();
        foreach (var item in orderItemsPerSeller)
        {
            var sellerID = item.Key;

            var shipmentActorGuid = IdMapping.GetShipmentActorGuid(sellerID.id, deliveryCityID);
            var shipmentActorID = new GrainID(shipmentActorGuid, typeof(SnapperTransactionalSimpleShipmentActor).FullName);
            var method = typeof(SnapperTransactionalSimpleShipmentActor).GetMethod("NewPackage");
            
            Debug.Assert(method != null);
            var call = new FunctionCall(method, (orderID, item.Value, deliveryCityID));
            tasks.Add(shipmentActorID, CallGrain(shipmentActorID, call, cxt));
           
            allOrderItems.AddRange(item.Value);
        }
        await Task.WhenAll(tasks.Values);

        // STEP 3: add package info to dictionary
        var packages = new List<PackageID>();
        foreach (var t in tasks)
        {
            var shipmentActorID = t.Key;

            Debug.Assert(t.Value.Result != null);
            (var packageID, var packageInfo) = ((PackageID, PackageInfo))t.Value.Result;
            Debug.Assert(packageID != null && packageInfo != null);

            packages.Add(packageID);

            // whenever the package is updated by the shipment actor, the update will forward to this order actor as well
            //orderActorState.packageInfo.Add(packageID, packageInfo);
            //orderActorState.dependencies.Add(packageID, shipmentActorID);
        }

        // STEP 4: add order info to dictionary
        var orderInfo = new OrderInfo(paymentInfo, allOrderItems, packages);
        //orderActorState.orderInfo.Add(orderID, orderInfo);
    }
}
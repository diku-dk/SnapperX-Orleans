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

public class NonTransactionalKeyValueOrderActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValueOrderActor
{
    int customerID;

    int baseCityID;

    GeneralStringKey nextOrderIDKey;

    public NonTransactionalKeyValueOrderActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValueOrderActor).FullName, snapperClusterCache, snapperReplicaCache) { }

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        nextOrderIDKey = new GeneralStringKey("nextOrderID");
        (var dictionaryState, var _) = GetState(tid);
        (var succeed, var _) = dictionaryState.Put(nextOrderIDKey, new GeneralLongValue(0));
        Debug.Assert(succeed);
        return null;
    }

    /// <returns> grains accessed by this transactions </returns>
    public async Task<object?> NewOrder(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var paymentInfo, var orderItemsPerSeller, var deliveryCityID) = ((PaymentInfo, Dictionary<SellerID, List<OrderItem>>, int))obj;

        // STEP 1: get an order ID, and update nextOrderID
        (var dictionaryState, var listState) = GetState(tid);
        var res = dictionaryState.Get(nextOrderIDKey) as GeneralLongValue;
        Debug.Assert(res != null);

        var nextOrderID = res.value;
        var orderID = new OrderID(new CustomerID(customerID, baseCityID), nextOrderID);
        
        res.value++;
        (var succeed, var msg) = dictionaryState.Put(nextOrderIDKey, res);
        Debug.Assert(succeed);

        // STEP 2: forward to a shipment actor to create a package for each seller
        var allOrderItems = new List<OrderItem>();
        var tasks = new List<Task<object?>>();
        foreach (var item in orderItemsPerSeller)
        {
            var sellerID = item.Key;

            var shipmentActorGuid = IdMapping.GetShipmentActorGuid(sellerID.id, deliveryCityID);
            var shipmentActorID = new GrainID(shipmentActorGuid, typeof(NonTransactionalKeyValueShipmentActor).FullName);
            var method = typeof(NonTransactionalKeyValueShipmentActor).GetMethod("NewPackage");
            
            Debug.Assert(method != null);
            var call = new FunctionCall(method, (orderID, item.Value, deliveryCityID));
            tasks.Add(CallGrain(shipmentActorID, call, tid));
           
            allOrderItems.AddRange(item.Value);
        }
        await Task.WhenAll(tasks);

        // STEP 3: add package info to dictionary
        var packages = new List<PackageID>();
        foreach (var t in tasks)
        {
            Debug.Assert(t.Result != null);
            (var referenceInfo, var packageInfo) = ((ReferenceInfo, PackageInfo))t.Result;
            Debug.Assert(referenceInfo != null && packageInfo != null);

            var packageID = referenceInfo.key2 as PackageID;
            Debug.Assert(packageID != null);
            packages.Add(packageID);

            // whenever the package is updated by the shipment actor, the update will forward to this order actor as well
            (succeed, msg) = dictionaryState.PutKeyWithReference(referenceInfo, packageInfo);
            Debug.Assert(succeed);
        }

        // STEP 4: add order info to dictionary
        var orderInfo = new OrderInfo(paymentInfo, allOrderItems, packages);
        (succeed, msg) = dictionaryState.Put(orderID, orderInfo);
        Debug.Assert(succeed);

        return null;
    }
}
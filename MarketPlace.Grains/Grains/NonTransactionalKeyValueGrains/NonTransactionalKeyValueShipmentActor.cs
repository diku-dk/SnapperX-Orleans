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

public class NonTransactionalKeyValueShipmentActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValueShipmentActor
{
    int sellerID;

    int baseCityID;

    int cityID;

    GeneralStringKey nextPackageIDKey;

    public NonTransactionalKeyValueShipmentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValueShipmentActor).FullName, snapperClusterCache, snapperReplicaCache) {}

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        nextPackageIDKey = new GeneralStringKey("nextPackageID");
        (var dictionaryState, var _) = GetState(tid);
        (var succeed, var _) = dictionaryState.Put(nextPackageIDKey, new GeneralLongValue(0));
        Debug.Assert(succeed);
        return null;
    }

    public async Task<object?> NewPackage(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        (var orderID, var orderItems, var deliveryCityID) = ((OrderID, List<OrderItem>, int))obj;
        Debug.Assert(deliveryCityID == cityID);

        // STEP 1: get a package ID, and update nextPackageID
        (var dictionaryState, var listState) = GetState(tid);
        var res = dictionaryState.Get(nextPackageIDKey) as GeneralLongValue;
        Debug.Assert(res != null);

        var nextPackageID = res.value;
        var packageID = new PackageID(new SellerID(sellerID, baseCityID), deliveryCityID, nextPackageID);

        res.value++;
        (var succeed, var msg) = dictionaryState.Put(nextPackageIDKey, res);
        Debug.Assert(succeed);

        // STEP 2: create package info
        var shipmentInfo = new ShipmentInfo(DateTime.UtcNow, "package created");
        var productIDs = orderItems.Select(x => x.productID).ToList();
        var packageInfo = new PackageInfo(orderID, shipmentInfo, productIDs);
        (succeed, msg) = dictionaryState.Put(packageID, packageInfo);
        Debug.Assert(succeed);

        // STEP 3: register a reference, after committing the current transaction, any future updates to the package ID will also be forwarded to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(orderID.customerID.id, orderID.customerID.baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(NonTransactionalKeyValueOrderActor).FullName);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.ReplicateReference, myID.grainID, packageID, orderActorID, packageID, new DefaultUpdateFunction());
        dictionaryState.RegisterReference(referenceInfo);

        // STEP 4: write log
        //listState.Add(new PackageLine(orderID, orderItems, packageID, packageInfo));

        return (referenceInfo, packageInfo);
    }
}
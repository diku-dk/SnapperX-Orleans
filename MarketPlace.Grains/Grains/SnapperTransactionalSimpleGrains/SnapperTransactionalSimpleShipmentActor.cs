using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.Grains.SnapperTransactionalSimpleGrains;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains;

public class SnapperTransactionalSimpleShipmentActor : TransactionExecutionGrain, IShipmentActor
{
    int sellerID;

    int baseCityID;

    int cityID;

    SellerID seller;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleShipmentActor");
        return myKey;
    }

    public SnapperTransactionalSimpleShipmentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleShipmentActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) {}

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        Debug.Assert(baseCityID == cityID);

        seller = new SellerID(sellerID, baseCityID);
        var sellerActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(SnapperTransactionalSimpleSellerActor).FullName);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var shipmentActorState = new ShipmentActorState(
            new GeneralLongValue(0), 
            new Dictionary<PackageID, PackageInfo>(), 
            new Dictionary<PackageID, GrainID>(), 
            new Dictionary<SellerID, PackageInfo> { { seller, new PackageInfo()} },
            new Dictionary<SellerID, GrainID> { { seller, sellerActorID } });
        dictionaryState.Put(GetMyKey(), shipmentActorState);

        return null;
    }

    public async Task<object?> ReadSimpleState(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.Read, cxt);
        var shipmentActorState = dictionaryState.Get(GetMyKey()) as ShipmentActorState;
        Debug.Assert(shipmentActorState != null);

        return MarketPlaceSerializer.SerializeShipmentActorState(shipmentActorState.nextPackageID, shipmentActorState.packages, shipmentActorState.dependencies);
    }

    public async Task<object?> NewPackage(TransactionContext cxt, object? obj = null)
    { 
        Debug.Assert(obj != null);
        (var orderID, var orderItems, var deliveryCityID) = ((OrderID, List<OrderItem>, int))obj;
        Debug.Assert(deliveryCityID == cityID);

        // STEP 1: get a package ID, and update nextPackageID
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var shipmentActorState = dictionaryState.Get(GetMyKey()) as ShipmentActorState;
        Debug.Assert(shipmentActorState != null);

        var id = shipmentActorState.nextPackageID.value;
        var packageID = new PackageID(seller, deliveryCityID, id);
        shipmentActorState.nextPackageID.value++;

        // STEP 2: create package info
        var shipmentInfo = new ShipmentInfo(DateTime.UtcNow, "package created");
        var productIDs = orderItems.Select(x => x.productID).ToList();
        var packageInfo = new PackageInfo(orderID, shipmentInfo, productIDs);
        //shipmentActorState.packages.Add(packageID, packageInfo);

        // STEP 3: forward the newly created package info to seller actor
        Debug.Assert(shipmentActorState.lastCreatedPackagePerSeller.ContainsKey(seller));
        shipmentActorState.lastCreatedPackagePerSeller[seller] = packageInfo;
        var sellerActorID = shipmentActorState.followerSellerActors[seller];
        var method = typeof(SnapperTransactionalSimpleSellerActor).GetMethod("AggregateOrderInfo");
        Debug.Assert(method != null);
        var call = new FunctionCall(method, packageInfo);
        await CallGrain(sellerActorID, call, cxt);
        
        // STEP 4: register a reference, after committing the current transaction, any future updates to the package ID will also be forwarded to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(orderID.customerID.id, orderID.customerID.baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(SnapperTransactionalSimpleOrderActor).FullName);
        //shipmentActorState.dependencies.Add(packageID, orderActorID);

        // STEP 5: write log
        //listState.Add(new PackageLine(orderID, orderItems, packageID, packageInfo));

        return (packageID, packageInfo);
    }
}
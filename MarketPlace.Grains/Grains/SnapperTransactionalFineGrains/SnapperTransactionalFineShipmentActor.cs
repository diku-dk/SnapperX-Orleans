using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Common.State;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.Grains.SnapperTransactionalFineGrains;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalFineGrains;

public class SnapperTransactionalFineShipmentActor : TransactionExecutionGrain, IShipmentActor
{
    int sellerID;

    int baseCityID;

    int cityID;

    SellerID seller;

    GeneralStringKey nextPackageIDKey;

    public SnapperTransactionalFineShipmentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalFineShipmentActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) {}

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        Debug.Assert(baseCityID == cityID);

        seller = new SellerID(sellerID, baseCityID);
        var sellerActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(SnapperTransactionalFineSellerActor).FullName);

        nextPackageIDKey = new GeneralStringKey("nextPackageID");
        (var dictionaryState, var _) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode>{ { nextPackageIDKey, AccessMode.ReadWrite }, { seller, AccessMode.ReadWrite } });
        (var succeed, _) = dictionaryState.Put(nextPackageIDKey, new GeneralLongValue(0));
        Debug.Assert(succeed);

        (succeed, _) = dictionaryState.Put(seller, new PackageInfo());
        Debug.Assert(succeed);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.UpdateReference, myID.grainID, seller, sellerActorID, seller, new SellerDashBoardFunction());
        dictionaryState.RegisterReference(referenceInfo);
        return null;
    }

    public async Task<object?> NewPackage(TransactionContext cxt, object? obj = null)
    { 
        Debug.Assert(obj != null);
        (var orderID, var orderItems, var deliveryCityID) = ((OrderID, List<OrderItem>, int))obj;
        Debug.Assert(deliveryCityID == cityID);

        // STEP 1: get a package ID, and update nextPackageID
        (var dictionaryState, var listState) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { nextPackageIDKey, AccessMode.ReadWrite }, { seller, AccessMode.ReadWrite } });
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
        //(dictionaryState, _) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { packageID, AccessMode.ReadWrite } });
        //(succeed, msg) = dictionaryState.Put(packageID, packageInfo);
        //Debug.Assert(succeed);

        // STEP 3: forward the newly created package info to seller actor
        var lastCreatedPackage = dictionaryState.Get(seller) as PackageInfo;
        Debug.Assert(lastCreatedPackage != null);
        dictionaryState.Put(seller, packageInfo);

        // STEP 4: register a reference, after committing the current transaction, any future updates to the package ID will also be forwarded to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(orderID.customerID.id, orderID.customerID.baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(SnapperTransactionalFineOrderActor).FullName);
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.ReplicateReference, myID.grainID, packageID, orderActorID, packageID, new DefaultUpdateFunction());
        //dictionaryState.RegisterReference(referenceInfo);

        // STEP 5: write log
        //listState.Add(new PackageLine(orderID, orderItems, packageID, packageInfo));

        return (referenceInfo, packageInfo);
    }
}
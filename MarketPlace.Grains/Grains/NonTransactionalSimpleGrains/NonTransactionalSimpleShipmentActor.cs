using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using System.Diagnostics;

namespace MarketPlace.Grains.NonTransactionalSimpleGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class NonTransactionalSimpleShipmentActor : Grain, INonTransactionalSimpleShipmentActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;
    int cityID;

    SellerID seller;

    GeneralLongValue nextPackageID;
    Dictionary<PackageID, PackageInfo> packages;

    /// <summary> package ID, order actor ID </summary>
    Dictionary<PackageID, GrainID> dependencies;

    Dictionary<SellerID, PackageInfo> lastCreatedPackagePerSeller;
    Dictionary<SellerID, GrainID> followerSellerActors;

    public NonTransactionalSimpleShipmentActor(ISnapperClusterCache snapperClusterCache) => this.snapperClusterCache = snapperClusterCache;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(NonTransactionalSimpleShipmentActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        Debug.Assert(baseCityID == cityID);

        seller = new SellerID(sellerID, baseCityID);
        var sellerActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(NonTransactionalSimpleSellerActor).FullName);

        nextPackageID = new GeneralLongValue(0);
        packages = new Dictionary<PackageID, PackageInfo>();
        dependencies = new Dictionary<PackageID, GrainID>();
        lastCreatedPackagePerSeller = new Dictionary<SellerID, PackageInfo> { { seller, new PackageInfo() } };
        followerSellerActors = new Dictionary<SellerID, GrainID> { { seller, sellerActorID } };

        return Task.FromResult(new TransactionResult());
    }

    public Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        txnResult.SetResult(MarketPlaceSerializer.SerializeShipmentActorState(nextPackageID, packages, dependencies));
        return Task.FromResult(txnResult);
    }

    public async Task<TransactionResult> NewPackage(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var orderID, var orderItems, var deliveryCityID) = ((OrderID, List<OrderItem>, int))obj;
        Debug.Assert(deliveryCityID == cityID);

        // STEP 1: get a package ID, and update nextPackageID
        var id = nextPackageID.value;
        nextPackageID.value++;
        var packageID = new PackageID(new SellerID(sellerID, baseCityID), deliveryCityID, id);
        txnResult.AddGrain(myID);

        // STEP 2: create package info
        var shipmentInfo = new ShipmentInfo(DateTime.UtcNow, "package created");
        var productIDs = orderItems.Select(x => x.productID).ToList();
        var packageInfo = new PackageInfo(orderID, shipmentInfo, productIDs);
        //packages.Add(packageID, packageInfo);

        // STEP 3: forward the newly created package info to seller actor
        Debug.Assert(lastCreatedPackagePerSeller.ContainsKey(seller));
        lastCreatedPackagePerSeller[seller] = packageInfo;
        var sellerActorID = followerSellerActors[seller];
        var sellerActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleSellerActor>(snapperClusterCache, GrainFactory, myRegionID, sellerActorID);
        var res = await sellerActor.AggregateOrderInfo(packageInfo);
        txnResult.MergeResult(res);

        // STEP 4: register a reference, after committing the current transaction, any future updates to the package ID will also be forwarded to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(orderID.customerID.id, orderID.customerID.baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(NonTransactionalSimpleOrderActor).FullName);
        //dependencies.Add(packageID, orderActorID);

        // STEP 5: write log
        //listState.Add(new PackageLine(orderID, orderItems, packageID, packageInfo));

        txnResult.SetResult((packageID, packageInfo));
        return txnResult;
    }
}
using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using System.Diagnostics;
using Orleans.Transactions.Abstractions;

namespace MarketPlace.Grains.OrleansTransactionalGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalShipmentActor : Grain, IOrleansTransactionalShipmentActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;
    int cityID;

    SellerID seller;

    readonly ITransactionalState<GeneralLongValue> nextPackageID;
    readonly ITransactionalState<Dictionary<PackageID, PackageInfo>> packages;

    /// <summary> package ID, order actor ID </summary>
    readonly ITransactionalState<Dictionary<PackageID, GrainID>> dependencies;

    readonly ITransactionalState<Dictionary<SellerID, PackageInfo>> lastCreatedPackagePerSeller;
    readonly ITransactionalState<Dictionary<SellerID, GrainID>> followerSellerActors;

    public OrleansTransactionalShipmentActor(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(nextPackageID))] ITransactionalState<GeneralLongValue> nextPackageID,
        [TransactionalState(nameof(packages))] ITransactionalState<Dictionary<PackageID, PackageInfo>> packages,
        [TransactionalState(nameof(dependencies))] ITransactionalState<Dictionary<PackageID, GrainID>> dependencies,
        [TransactionalState(nameof(lastCreatedPackagePerSeller))] ITransactionalState<Dictionary<SellerID, PackageInfo>> lastCreatedPackagePerSeller,
        [TransactionalState(nameof(followerSellerActors))] ITransactionalState<Dictionary<SellerID, GrainID>> followerSellerActors)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.nextPackageID = nextPackageID ?? throw new ArgumentException(nameof(nextPackageID));
        this.packages = packages ?? throw new ArgumentException(nameof(packages));
        this.dependencies = dependencies ?? throw new ArgumentException(nameof(dependencies));
        this.lastCreatedPackagePerSeller = lastCreatedPackagePerSeller ?? throw new ArgumentException(nameof(lastCreatedPackagePerSeller));
        this.followerSellerActors = followerSellerActors ?? throw new ArgumentException(nameof(followerSellerActors));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalShipmentActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, cityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        Debug.Assert(obj != null);
        baseCityID = (int)obj;

        Debug.Assert(baseCityID == cityID);

        seller = new SellerID(sellerID, baseCityID);
        var sellerActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(OrleansTransactionalSellerActor).FullName);

        await nextPackageID.PerformUpdate(x => x = new GeneralLongValue(0));
        await packages.PerformUpdate(x => x = new Dictionary<PackageID, PackageInfo>());
        await dependencies.PerformUpdate(x => x = new Dictionary<PackageID, GrainID>());
        await lastCreatedPackagePerSeller.PerformUpdate(x => x = new Dictionary<SellerID, PackageInfo>());
        await followerSellerActors.PerformUpdate(x => x = new Dictionary<SellerID, GrainID>());

        await lastCreatedPackagePerSeller.PerformUpdate(x => x.Add(seller, new PackageInfo()));
        await followerSellerActors.PerformUpdate(x => x.Add(seller, sellerActorID));

        return new TransactionResult();
    }

    public async Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        var item1 = await nextPackageID.PerformRead(x => x);
        var item2 = await packages.PerformRead(x => x);
        var item3 = await dependencies.PerformRead(x => x);
        txnResult.SetResult(MarketPlaceSerializer.SerializeShipmentActorState(item1, item2, item3));
        return txnResult;
    }

    /// <summary> this method is called by the order actor </summary>
    public async Task<TransactionResult> NewPackage(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var orderID, var orderItems, var deliveryCityID) = ((OrderID, List<OrderItem>, int))obj;
        Debug.Assert(deliveryCityID == cityID);

        // STEP 1: get a package ID, and update nextPackageID
        var id = await nextPackageID.PerformUpdate(x => { var i = x.value; x.value++; return i; });
        var packageID = new PackageID(new SellerID(sellerID, baseCityID), deliveryCityID, id);
        txnResult.AddGrain(myID);

        // STEP 2: create package info
        var shipmentInfo = new ShipmentInfo(DateTime.UtcNow, "package created");
        var productIDs = orderItems.Select(x => x.productID).ToList();
        var packageInfo = new PackageInfo(orderID, shipmentInfo, productIDs);

        /*
        await packages.PerformUpdate(x =>
        {
            Debug.Assert(!x.ContainsKey(packageID));
            x.Add(packageID, packageInfo);
        });
        */

        // STEP 3: forward the newly created package info to seller actor
        await lastCreatedPackagePerSeller.PerformUpdate(x => 
        {
            Debug.Assert(x.ContainsKey(seller));
            x[seller] = packageInfo;
        });

        var sellerActorID = await followerSellerActors.PerformRead(x => 
        {
            Debug.Assert(x.ContainsKey(seller));
            return x[seller];
        });

        var sellerActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalSellerActor>(snapperClusterCache, GrainFactory, myRegionID, sellerActorID);
        var res = await sellerActor.AggregateOrderInfo(packageInfo);
        txnResult.MergeResult(res);

        // STEP 4: register a reference, after committing the current transaction, any future updates to the package ID will also be forwarded to the order actor
        var orderActorGuid = IdMapping.GetOrderActorGuid(orderID.customerID.id, orderID.customerID.baseCityID);
        var orderActorID = new GrainID(orderActorGuid, typeof(OrleansTransactionalOrderActor).FullName);
        //await dependencies.PerformUpdate(x => x.Add(packageID, orderActorID));

        // STEP 5: write log
        //listState.Add(new PackageLine(orderID, orderItems, packageID, packageInfo));

        txnResult.SetResult((packageID, packageInfo));
        return txnResult;
    }
}
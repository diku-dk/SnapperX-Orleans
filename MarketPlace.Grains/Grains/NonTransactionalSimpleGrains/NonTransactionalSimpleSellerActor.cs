using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using System.Diagnostics;
using MarketPlace.Grains.OrleansTransactionalGrains;

namespace MarketPlace.Grains.NonTransactionalSimpleGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class NonTransactionalSimpleSellerActor : Grain, INonTransactionalSimpleSellerActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;

    MyCounter totalNumOrder;
    MyCounter totalNumProduct;
    HashSet<GrainID> dependencies;

    public NonTransactionalSimpleSellerActor(ISnapperClusterCache snapperClusterCache) => this.snapperClusterCache = snapperClusterCache;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalSellerActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        var shipmentActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(OrleansTransactionalShipmentActor).FullName);

        totalNumOrder = new MyCounter();
        totalNumProduct = new MyCounter();
        dependencies = new HashSet<GrainID> { shipmentActorID };

        return Task.FromResult(new TransactionResult());
    }

    public Task<TransactionResult> AggregateOrderInfo(object? obj = null)
    {
        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);

        Debug.Assert(obj != null);
        var packageInfo = obj as PackageInfo;
        Debug.Assert(packageInfo != null);

        totalNumOrder.Add(1);
        totalNumProduct.Add(packageInfo.items.Count);

        return Task.FromResult(txnResult);
    }
}
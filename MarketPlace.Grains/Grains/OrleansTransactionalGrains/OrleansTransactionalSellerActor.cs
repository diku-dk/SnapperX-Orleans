using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using Orleans.Transactions.Abstractions;
using System.Diagnostics;
using MarketPlace.Grains.ValueState;

namespace MarketPlace.Grains.OrleansTransactionalGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalSellerActor : Grain, IOrleansTransactionalSellerActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int sellerID;
    int baseCityID;

    readonly ITransactionalState<MyCounter> totalNumOrder;
    readonly ITransactionalState<MyCounter> totalNumProduct;
    readonly ITransactionalState<HashSet<GrainID>> dependencies;

    public OrleansTransactionalSellerActor(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(totalNumOrder))] ITransactionalState<MyCounter> totalNumOrder,
        [TransactionalState(nameof(totalNumProduct))] ITransactionalState<MyCounter> totalNumProduct,
        [TransactionalState(nameof(dependencies))] ITransactionalState<HashSet<GrainID>> dependencies)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.totalNumOrder = totalNumOrder ?? throw new ArgumentException(nameof(totalNumOrder));
        this.totalNumProduct = totalNumProduct ?? throw new ArgumentException(nameof(totalNumProduct));
        this.dependencies = dependencies ?? throw new ArgumentException(nameof(dependencies));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(OrleansTransactionalSellerActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        var shipmentActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(OrleansTransactionalShipmentActor).FullName);

        await totalNumOrder.PerformUpdate(x => x = new MyCounter());
        await totalNumProduct.PerformUpdate(x => x = new MyCounter());
        await dependencies.PerformUpdate(x => x = new HashSet<GrainID>());

        await dependencies.PerformUpdate(x => x.Add(shipmentActorID));

        return new TransactionResult();
    }

    public async Task<TransactionResult> AggregateOrderInfo(object? obj = null)
    {
        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);

        Debug.Assert(obj != null);
        var packageInfo = obj as PackageInfo;
        Debug.Assert(packageInfo != null);

        await totalNumOrder.PerformUpdate(x => x.Add(1));
        await totalNumProduct.PerformUpdate(x => x.Add(packageInfo.items.Count));

        return txnResult;
    }
}
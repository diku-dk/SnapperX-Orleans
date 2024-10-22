using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.SnapperTransactionalSimpleGrains;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains;

public class SnapperTransactionalSimpleSellerActor : TransactionExecutionGrain, ISellerActor
{
    int sellerID;

    int baseCityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleSellerActor");
        return myKey;
    }

    public SnapperTransactionalSimpleSellerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleSellerActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        var shipmentActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(SnapperTransactionalSimpleShipmentActor).FullName);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var sellerActorState = new SellerActorState(new MyCounter(), new MyCounter(), new HashSet<GrainID> { shipmentActorID } );
        dictionaryState.Put(GetMyKey(), sellerActorState);
        return null;
    }

    public async Task<object?> AggregateOrderInfo(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var packageInfo = obj as PackageInfo;
        Debug.Assert(packageInfo != null);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var sellerActorState = dictionaryState.Get(GetMyKey()) as SellerActorState;
        Debug.Assert(sellerActorState != null);

        sellerActorState.totalNumOrder.Add(1);
        sellerActorState.totalNumProduct.Add(packageInfo.items.Count);

        return null;
    }
}
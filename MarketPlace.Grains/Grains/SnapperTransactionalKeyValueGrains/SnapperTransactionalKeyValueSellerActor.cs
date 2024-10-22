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

namespace MarketPlace.Grains.SnapperTransactionalKeyValueGrains;

public class SnapperTransactionalKeyValueSellerActor : TransactionExecutionGrain, ISellerActor
{
    int sellerID;

    int baseCityID;

    SellerID seller;

    public SnapperTransactionalKeyValueSellerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalKeyValueSellerActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        seller = new SellerID(sellerID, baseCityID);
        var shipmentActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(SnapperTransactionalKeyValueShipmentActor).FullName);

        (var dictionaryState, var _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey> { seller });
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.UpdateReference, shipmentActorID, seller, myID.grainID, seller, new SellerDashBoardFunction());
        (var succeed, _) = dictionaryState.PutKeyWithReference(referenceInfo, new SellerInfo());
        Debug.Assert(succeed);
        return null;
    }
}
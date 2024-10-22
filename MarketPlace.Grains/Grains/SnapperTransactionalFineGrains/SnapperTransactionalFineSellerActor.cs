using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using Concurrency.Common;
using Concurrency.Common.State;
using MarketPlace.Grains.Grains.SnapperTransactionalFineGrains;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalFineGrains;

public class SnapperTransactionalFineSellerActor : TransactionExecutionGrain, ISellerActor
{
    int sellerID;

    int baseCityID;

    SellerID seller;

    public SnapperTransactionalFineSellerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalFineSellerActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (sellerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        seller = new SellerID(sellerID, baseCityID);
        var shipmentActorID = new GrainID(Helper.ConvertIntToGuid(sellerID, baseCityID), typeof(SnapperTransactionalFineShipmentActor).FullName);

        (var dictionaryState, var _) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { seller, AccessMode.ReadWrite } });
        var referenceInfo = new ReferenceInfo(SnapperKeyReferenceType.UpdateReference, shipmentActorID, seller, myID.grainID, seller, new SellerDashBoardFunction());
        (var succeed, _ ) = dictionaryState.PutKeyWithReference(referenceInfo, new SellerInfo());
        Debug.Assert(succeed);
        return null;
    }
}
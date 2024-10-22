using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.KeyState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains;

public class SnapperTransactionalSimpleCustomerActor : TransactionExecutionGrain, ICustomerActor
{
    int customerID;

    int baseCityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleCustomerActor");
        return myKey;
    }

    public SnapperTransactionalSimpleCustomerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache,IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleCustomerActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);

        return null;
    }
}
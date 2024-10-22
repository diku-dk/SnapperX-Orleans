using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Common.State;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalKeyValueGrains;

public class SnapperTransactionalKeyValueCustomerActor : TransactionExecutionGrain, ICustomerActor
{
    int customerID;

    int baseCityID;

    CustomerID id;

    public SnapperTransactionalKeyValueCustomerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache,IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalKeyValueCustomerActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        id = new CustomerID(customerID, baseCityID);
        var info = new GeneralLongValue(0);
        (var dictionaryState, _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ id });
        (var succeed, var _) = dictionaryState.Put(id, info);
        Debug.Assert(succeed);
        return null;
    }
}
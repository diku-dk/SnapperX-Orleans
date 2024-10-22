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

namespace MarketPlace.Grains.SnapperTransactionalFineGrains;

public class SnapperTransactionalFineCustomerActor : TransactionExecutionGrain, ICustomerActor
{
    int customerID;

    int baseCityID;

    CustomerID id;

    public SnapperTransactionalFineCustomerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache,IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalFineCustomerActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        id = new CustomerID(customerID, baseCityID);
        var info = new GeneralLongValue(0);
        (var dictionaryState, _) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { id, AccessMode.ReadWrite } });
        (var succeed, var _) = dictionaryState.Put(id, info);
        Debug.Assert(succeed);
        return null;
    }
}
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

public class SnapperTransactionalKeyValuePaymentActor : TransactionExecutionGrain, IPaymentActor
{
    int customerID;

    int baseCityID;

    CustomerID id;

    public SnapperTransactionalKeyValuePaymentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalKeyValuePaymentActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        id = new CustomerID(customerID, baseCityID);
        var paymentMethod = new PaymentMethod(id.id, "paymentService");
        (var dictionaryState, _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ id });
        (var succeed, var _) = dictionaryState.Put(id, paymentMethod);
        Debug.Assert(succeed);
        return null;
    }

    public async Task<object?> ProcessPayment(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        (var paymentMethod, var money) = ((PaymentMethod, double))obj;

        (var dictionaryState, _) = await GetKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ id });
        var info = dictionaryState.Get(id);
        Debug.Assert(info != null);

        var method = info as PaymentMethod;
        Debug.Assert(method != null);
        Debug.Assert(method.Equals(paymentMethod));

        return (true, "");
    }
}
using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;

namespace MarketPlace.Grains.SnapperTransactionalFineGrains;

public class SnapperTransactionalFinePaymentActor : TransactionExecutionGrain, IPaymentActor
{
    int customerID;

    int baseCityID;

    CustomerID id;

    public SnapperTransactionalFinePaymentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalFinePaymentActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);

        id = new CustomerID(customerID, baseCityID);
        var paymentMethod = new PaymentMethod(id.id, "paymentService");
        (var dictionaryState, _) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { id, AccessMode.ReadWrite } });
        (var succeed, var _) = dictionaryState.Put(id, paymentMethod);
        Debug.Assert(succeed);
        return null;
    }

    public async Task<object?> ProcessPayment(TransactionContext cxt, object? obj = null)
    { 
        Debug.Assert(obj != null);
        (var paymentMethod, var money) = ((PaymentMethod, double))obj;

        (var dictionaryState, _) = await GetFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { id, AccessMode.ReadWrite } });
        var info = dictionaryState.Get(id);
        Debug.Assert(info != null);

        var method = info as PaymentMethod;
        Debug.Assert(method != null);
        Debug.Assert(method.Equals(paymentMethod));

        return (true, "");
    }
}
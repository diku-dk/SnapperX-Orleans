using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.ILogging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains;

public class SnapperTransactionalSimplePaymentActor : TransactionExecutionGrain, IPaymentActor
{
    int customerID;

    int baseCityID;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimplePaymentActor");
        return myKey;
    }

    public SnapperTransactionalSimplePaymentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimplePaymentActor).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        (var dictionaryState, var _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        return null;
    }

    public async Task<object?> ProcessPayment(TransactionContext cxt, object? obj = null)
    { 
        Debug.Assert(obj != null);
        (var paymentMethod, var money) = ((PaymentMethod, double))obj;

        _ = await GetSimpleState(AccessMode.ReadWrite, cxt);
        return (true, "");
    }
}
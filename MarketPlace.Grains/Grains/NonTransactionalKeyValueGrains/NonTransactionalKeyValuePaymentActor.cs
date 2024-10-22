using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Implementation.DataModel;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalKeyValueGrains;

public class NonTransactionalKeyValuePaymentActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValuePaymentActor
{
    int customerID;

    int baseCityID;

    public NonTransactionalKeyValuePaymentActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValuePaymentActor).FullName, snapperClusterCache, snapperReplicaCache) { }

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        return null;
    }

    public async Task<object?> ProcessPayment(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        (var paymentMethod, var money) = ((PaymentMethod, double))obj;

        _ = GetState(tid);
        return (true, "");
    }
}
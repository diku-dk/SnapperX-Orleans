using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Implementation.DataModel;
using MarketPlace.Interfaces;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalKeyValueGrains;

public class NonTransactionalKeyValueCustomerActor : NonTransactionalKeyValueGrain, INonTransactionalKeyValueCustomerActor
{
    int customerID;

    int baseCityID;

    public NonTransactionalKeyValueCustomerActor(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValueCustomerActor).FullName, snapperClusterCache, snapperReplicaCache) { }

    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;
        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        return null;
    }
}
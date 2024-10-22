using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using MarketPlace.Interfaces;
using Replication.Implementation.TransactionReplication;
using Replication.Interface;

namespace MarketPlace.Grains.SnapperReplicatedKeyValueGrains;

public class SnapperReplicatedKeyValueStockActor : TransactionReplicationGrain, IReplicaStockActor
{
    int sellerID;

    int cityID;

    public SnapperReplicatedKeyValueStockActor(IHistoryManager historyManager, ISnapperReplicaCache snapperReplicaCache, ISnapperClusterCache snapperClusterCache)
        : base(typeof(SnapperReplicatedKeyValueStockActor).FullName, historyManager, snapperReplicaCache, snapperClusterCache) { }

    public async Task<object?> CheckStock(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, var _) = await GetState(cxt);
        return SnapperSerializer.Serialize(dictionaryState.GetAllItems());
    }
}
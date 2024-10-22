using Concurrency.Common;
using Concurrency.Common.ICache;
using Replication.Implementation.TransactionReplication;
using Replication.Interface;
using SmallBank.Interfaces;

namespace SmallBank.Grains;

public class SnapperReplicatedKeyValueAccountGrain : TransactionReplicationGrain, ISnapperReplicatedAccountGrain
{
    public SnapperReplicatedKeyValueAccountGrain(IHistoryManager historyManager, ISnapperReplicaCache snapperReplicaCache, ISnapperClusterCache snapperClusterCache)
        : base(typeof(SnapperReplicatedKeyValueAccountGrain).FullName, historyManager, snapperReplicaCache, snapperClusterCache) { }

    public async Task<object?> GetBalance(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, _) = await GetState(cxt);
        return null;
    }

    public async Task<object?> CollectBalance(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, _) = await GetState(cxt);
        return null;
    }
}
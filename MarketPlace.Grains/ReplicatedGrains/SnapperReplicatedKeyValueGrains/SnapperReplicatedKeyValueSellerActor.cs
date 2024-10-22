using Concurrency.Common;
using Concurrency.Common.ICache;
using MarketPlace.Interfaces;
using Replication.Implementation.TransactionReplication;
using Replication.Interface;
using System.Diagnostics;

namespace MarketPlace.Grains.SnapperReplicatedKeyValueGrains;

public class SnapperReplicatedKeyValueSellerActor : TransactionReplicationGrain, IReplicaSellerActor
{
    public SnapperReplicatedKeyValueSellerActor(IHistoryManager historyManager, ISnapperReplicaCache snapperReplicaCache, ISnapperClusterCache snapperClusterCache)
        : base(typeof(SnapperReplicatedKeyValueSellerActor).FullName, historyManager, snapperReplicaCache, snapperClusterCache) { }

    public async Task<object?> RefreshSellerDashboard(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var stockActors = obj as List<GrainID>;
        Debug.Assert(stockActors != null);

        var method = typeof(SnapperReplicatedKeyValueStockActor).GetMethod("CheckStock");
        Debug.Assert(method != null);
        var call = new FunctionCall(method);

        var tasks = new List<Task<object?>>();
        foreach (var stockActorID in stockActors) tasks.Add(CallGrain(stockActorID, call, cxt));
        await Task.WhenAll(tasks);

        return tasks.Select(x => x.Result).ToList();
    }
}
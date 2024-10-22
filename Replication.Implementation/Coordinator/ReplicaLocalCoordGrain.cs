using Concurrency.Common;
using Concurrency.Common.ICache;
using Orleans.Concurrency;
using Replication.Implementation.GrainReplicaPlacement;
using Replication.Interface;
using Replication.Interface.Coordinator;
using System.Diagnostics;
using Utilities;

namespace Replication.Implementation.Coordinator;

[Reentrant]
[SnapperReplicaGrainPlacementStrategy]
internal class ReplicaLocalCoordGrain : Grain, IReplicaLocalCoordGrain
{
    // coord basic info
    Guid myID;
    string myRegionID;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly IHistoryManager historyManager;

    // PACT
    List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList;
    SortedDictionary<SnapperID, Batch> batches;               // key: regional bid, sent from regional coordinators

    public ReplicaLocalCoordGrain(ISnapperClusterCache snapperClusterCache, IHistoryManager historyManager)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.historyManager = historyManager;
    }

    public override Task OnActivateAsync(CancellationToken _)
    {
        myID = this.GetPrimaryKey();
        var strs = this.GetPrimaryKeyString().Split('+');
        myRegionID = strs[1];
        Debug.Assert(strs[2].Equals(RuntimeIdentity));

        pactList = new List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)>();
        batches = new SortedDictionary<SnapperID, Batch>();

        // periodically generate a new lcoal batch
        var batchSizeInMSecs = snapperClusterCache.GetBatchSize(Hierarchy.Local);
        RegisterTimer(GenerateBatch, 0, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(batchSizeInMSecs));

        return Task.CompletedTask;
    }

    public Task CheckGC()
    {
        if (pactList.Count != 0) Console.WriteLine($"ReplicaLocalCoord: pactList.Count = {pactList.Count}. ");
        if (batches.Count != 0) Console.WriteLine($"ReplicaLocalCoord: batches.Count = {batches.Count}. ");
        return Task.CompletedTask;
    }

    public Task RegisterAccessInfo(SnapperID id, bool multiSilo, HashSet<SnapperGrainID> grains)
    {
        historyManager.RegisterAccessInfo(id, multiSilo, grains);
        return Task.CompletedTask;
    }

    public async Task<(SnapperID, SnapperID)> NewPACT(ActorAccessInfo actorAccessInfo)
    {
        var promise = new TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>();
        pactList.Add((promise, actorAccessInfo));

        (var bid, var tid, _) = await promise.Task;
        return (bid, tid);
    }

    async Task GenerateBatch(object _)
    {
        // generate a new local batch
        await historyManager.GenerateBatch(pactList);

        // process received up level batches
        await historyManager.ProcessBatches(batches, new Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>>());
    }

    /// <summary> receive regional batch (contains single-home multi-silo transactions) from regional coordinators </summary>
    public Task ReceiveBatch(Batch batch)
    {
        Debug.Assert(!batch.regional_bid.isEmpty() && batch.local_bid.isEmpty());
        Debug.Assert(batch.txnList.Count != 0);
        batches.Add(batch.regional_bid, batch);
        return Task.CompletedTask;
    }
}
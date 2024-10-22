using Concurrency.Common;
using Concurrency.Common.ICache;
using Orleans.Concurrency;
using Replication.Implementation.GrainReplicaPlacement;
using Replication.Interface;
using Replication.Interface.Coordinator;
using System.Diagnostics;

namespace Replication.Implementation.Coordinator;

[Reentrant]
[SnapperReplicaGrainPlacementStrategy]
internal class ReplicaRegionalCoordGrain : Grain, IReplicaRegionalCoordGrain
{
    // coord basic info
    Guid myID;
    string myRegionID;
    readonly IHistoryManager historyManager;
    readonly ISnapperClusterCache snapperClusterCache;

    // PACT
    List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList;

    public ReplicaRegionalCoordGrain(IHistoryManager historyManager, ISnapperClusterCache snapperClusterCache)
    { 
        this.historyManager = historyManager; 
        this.snapperClusterCache = snapperClusterCache;
    }

    public override Task OnActivateAsync(CancellationToken _)
    {
        myID = this.GetPrimaryKey();
        var strs = this.GetPrimaryKeyString().Split('+');
        myRegionID = strs[1];
        Debug.Assert(strs[2].Equals(RuntimeIdentity));

        pactList = new List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)>();

        // periodically generate a new lcoal batch
        var batchSizeInMSecs = 5;//snapperClusterCache.GetBatchSize(Hierarchy.Regional);
        RegisterTimer(GenerateBatch, 0, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(batchSizeInMSecs));

        return Task.CompletedTask;
    }

    public Task CheckGC()
    {
        if (pactList.Count != 0) Console.WriteLine($"ReplicaRegionalCoord: pactList.Count = {pactList.Count}. ");
        return Task.CompletedTask;
    }

    public async Task<SnapperID> NewACT() => await historyManager.NewACT();

    public Task RegisterAccessInfo(SnapperID id, int numSilos)
    {
        historyManager.RegisterAccessInfo(id, numSilos);
        return Task.CompletedTask;
    }

    public async Task ACKCompletion(SnapperID id) => await historyManager.ACKCompletion(id);

    public async Task<(SnapperID, SnapperID)> NewPACT(ActorAccessInfo actorAccessInfo)
    {
        var promise = new TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>();
        pactList.Add((promise, actorAccessInfo));

        (var bid, var tid, _) = await promise.Task;
        return (bid, tid);
    }

    async Task GenerateBatch(object _)
    {
        // generate a new regional batch
        var schedule = await historyManager.GenerateBatch(pactList);

        // emit the regional batch info to local silos
        if (schedule.Count == 0) return;

        var tasks = new List<Task>();
        foreach (var item in schedule)
        {
            var localCoord = GrainFactory.GetGrain<IReplicaLocalCoordGrain>(item.Key.grainID.id, item.Key.location);
            tasks.Add(localCoord.ReceiveBatch(item.Value));
        }
        await Task.WhenAll(tasks);
    }
}
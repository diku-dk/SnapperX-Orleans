using Utilities;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Orleans.Concurrency;
using System.Diagnostics;
using Concurrency.Common;
using Concurrency.Interface;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;

namespace Concurrency.Implementation.Coordinator;

using Schedule = Dictionary<SnapperGrainID, Batch>;

[Reentrant]
[SnapperGrainPlacementStrategy]
public class RegionalCoordGrain : Grain, IRegionalCoordGrain
{
    // coord basic info
    Guid myID;
    string myRegionID;
    SnapperGrainID myGrainID;
    CoordinatorManager coordManager;

    readonly ISnapperLoggingHelper log;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly IScheduleManager scheduleManager;
    
    IRegionalCoordGrain neighbor;
    Stopwatch watch;
    double batchSizeInMSecs;

    CommitInfo commitInfo;

    ExperimentData experimentData;

    public RegionalCoordGrain(ISnapperLoggingHelper log, ISnapperClusterCache snapperClusterCache, IScheduleManager scheduleManager)
    {
        this.log = log;
        this.snapperClusterCache = snapperClusterCache;
        this.scheduleManager = scheduleManager;
    }

    public Task Init() => Task.CompletedTask;

    public Task<AggregatedExperimentData> CheckGC()
    {
        coordManager.CheckGC();

        var result = experimentData.AggregateAndClear();
        return Task.FromResult(result);
    }

    public override async Task OnActivateAsync(CancellationToken _)
    {
        myID = this.GetPrimaryKey();
        var strs = this.GetPrimaryKeyString().Split('+');
        myRegionID = strs[1];
        if (!strs[2].Equals(RuntimeIdentity)) return;
        myGrainID = new SnapperGrainID(myID, myRegionID + "+" + RuntimeIdentity);

        commitInfo = new CommitInfo();
        experimentData = new ExperimentData();

        (var _, var speculativeBatch) = snapperClusterCache.IsSpeculative();
        (var globalCoords, var regionalCoords, var localCoords) = scheduleManager.GetAllCoordInfo();
        coordManager = new CoordinatorManager(myGrainID, RuntimeIdentity, myRegionID, Hierarchy.Regional, speculativeBatch, GrainFactory, log, commitInfo, experimentData, globalCoords, regionalCoords, localCoords);

        (var isFirst, var neighborGuid) = snapperClusterCache.GetNeighbor(Hierarchy.Regional, myRegionID, RuntimeIdentity, myID);
        neighbor = GrainFactory.GetGrain<IRegionalCoordGrain>(neighborGuid, myRegionID + "+" + RuntimeIdentity);

        watch = new Stopwatch();
        watch.Start();
        batchSizeInMSecs = snapperClusterCache.GetBatchSize(Hierarchy.Regional);
        Console.WriteLine($"RegionalCoord {myGrainID.grainID.Print()}: neighbor = {Helper.ConvertGuidToInt(neighborGuid)}, batchSize = {batchSizeInMSecs}ms");

        if (isFirst) await neighbor.PassToken();
    }

    public async Task<(SnapperID, SnapperID, List<SnapperGrainID>, CommitInfo)> NewPACT(Immutable<ActorAccessInfo> actorAccessInfo)
    {
        var time = DateTime.UtcNow;
        var promise = new TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>();
        coordManager.pactList.Add((promise, actorAccessInfo.Value));
        (var bid, var tid, var services) = await promise.Task;
        experimentData.Set(MonitorTime.NewRegionalPACT, (DateTime.UtcNow - time).TotalMilliseconds);
        return (bid, tid, services, commitInfo);
    }

    public async Task PassToken()
    {
        scheduleManager.UpdateCommitInfo(commitInfo);

        if (watch.ElapsedMilliseconds < batchSizeInMSecs) _ = neighbor.PassToken();
        else
        {
            var schedules = new List<Schedule>();

            // generate a new local batch
            if (coordManager.pactList.Count != 0)
            {
                var time = DateTime.UtcNow;
                var info = scheduleManager.GenerateBatch(myGrainID, coordManager.pactList);
                coordManager.AddBatchMetadata(info.Item1, info.Item2, info.Item3, info.Item4);
                schedules.Add(info.Item4);
                experimentData.Set(MonitorTime.GenerateRegionalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
            }

            // process received up level batches
            if (coordManager.batches.Count != 0)
            {
                var infos = scheduleManager.ProcessBatches(myGrainID, coordManager.batches, coordManager.higherPACTList);
                foreach (var info in infos)
                {
                    coordManager.AddBatchMetadata(info.Item1, info.Item2, info.Item3, info.Item4);
                    schedules.Add(info.Item4);
                }
            }
            
            _ = neighbor.PassToken();

            if (schedules.Count != 0)
            {
                watch.Restart();
                await EmitSchedule(schedules);
            }
        }
    }

    async Task EmitSchedule(List<Schedule> schedules)
    {
        // emit the regional batch info to local silos
        foreach (var schedule in schedules)
        {
            var primaryBid = schedule.First().Value.GetPrimaryBid();

            if (log.DoLogging())
            {
                var grains = schedule.Keys.Select(x => new GrainID(x.grainID.id, x.grainID.className)).ToHashSet();
                await log.InfoLog(primaryBid, grains);
            }

            var time = DateTime.UtcNow;
            var tasks = new List<Task>();
            foreach (var item in schedule)
            {
                item.Value.coordID = myGrainID;
                var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(item.Key.grainID.id, item.Key.location);
                tasks.Add(localCoord.ReceiveBatch(item.Value, commitInfo));
            }
            await Task.WhenAll(tasks);
            experimentData.Set(MonitorTime.EmitRegionalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
        }
    }

    public async Task<(List<SnapperGrainID>, CommitInfo)> NewPACT(SnapperID primaryBid, SnapperID primaryTid, Immutable<ActorAccessInfo> actorAccessInfo)
    {
        coordManager.ReceivePACTInfo(primaryBid, primaryTid, actorAccessInfo.Value);

        var promise = new TaskCompletionSource<List<SnapperGrainID>>();
        coordManager.higherPACTList.Add(primaryTid, promise);
        var selectedlocalCoords = await promise.Task;

        return (selectedlocalCoords, commitInfo);
    }

    /// <summary> receive regional batch (contains multi-home / single-home multi-silo transactions) from regional coordinators </summary>
    public Task ReceiveBatch(Batch batch, CommitInfo highCommitInfo)
    {
        commitInfo.MergeCommitInfo(highCommitInfo);
        coordManager.ReceiveBatchInfo(batch);
        return Task.CompletedTask;
    }

    public async Task ACKBatchComplete(SnapperID primaryBid, HashSet<GrainID> writers)
    {
        var collectedWriters = coordManager.writerGrainsPerBatch[primaryBid];
        collectedWriters.UnionWith(writers);

        if (coordManager.numACKsPerBatch[primaryBid].Signal()) await coordManager.ReadyToCommitBatch(primaryBid);
    }

    public async Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid)
    {
        var time = DateTime.UtcNow;
        await coordManager.WaitForBatchCommit(primaryBid, bid);
        experimentData.Set(MonitorTime.CommitRegionalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
    }

    public Task CommitHighBatch(SnapperID primaryBid, CommitInfo highCommitInfo)
    {
        commitInfo.MergeCommitInfo(highCommitInfo);
        coordManager.CommitHighBatch(primaryBid);
        return Task.CompletedTask;
    }
}
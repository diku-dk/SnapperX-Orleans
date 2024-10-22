using Utilities;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Orleans.Concurrency;
using System.Diagnostics;
using Concurrency.Common;
using Concurrency.Interface;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using Concurrency.Interface.TransactionExecution;

namespace Concurrency.Implementation.Coordinator;

using Schedule = Dictionary<SnapperGrainID, Batch>;

[Reentrant]
[SnapperGrainPlacementStrategy]
public class LocalCoordGrain : Grain, ILocalCoordGrain
{
    // coord basic info
    Guid myID;
    string myRegionID;
    SnapperGrainID myGrainID;
    CoordinatorManager coordManager;

    readonly ISnapperLoggingHelper log;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly IScheduleManager scheduleManager;

    ILocalCoordGrain neighbor;
    Stopwatch watch;
    double batchSizeInMSecs;

    CommitInfo commitInfo;

    ExperimentData experimentData;

    public LocalCoordGrain(ISnapperLoggingHelper log, ISnapperClusterCache snapperClusterCache, IScheduleManager scheduleManager)
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
        coordManager = new CoordinatorManager(myGrainID, RuntimeIdentity, myRegionID, Hierarchy.Local, speculativeBatch, GrainFactory, log, commitInfo, experimentData, globalCoords, regionalCoords, localCoords);

        (var isFirst, var neighborGuid) = snapperClusterCache.GetNeighbor(Hierarchy.Local, myRegionID, RuntimeIdentity, myID);
        neighbor = GrainFactory.GetGrain<ILocalCoordGrain>(neighborGuid, myRegionID + "+" + RuntimeIdentity);

        watch = new Stopwatch();
        watch.Start();
        batchSizeInMSecs = snapperClusterCache.GetBatchSize(Hierarchy.Local);
        Console.WriteLine($"LocalCoord {myGrainID.grainID.Print()} in silo {RuntimeIdentity}: neighbor = {Helper.ConvertGuidToInt(neighborGuid)}, batchSize = {batchSizeInMSecs}ms");

        if (isFirst) await neighbor.PassToken();
    }

    public async Task<(SnapperID, SnapperID, List<SnapperGrainID>, CommitInfo)> NewPACT(Immutable<ActorAccessInfo> actorAccessInfo)
    {
        var time = DateTime.UtcNow;
        var promise = new TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>();
        coordManager.pactList.Add((promise, actorAccessInfo.Value));
        (var bid, var tid, var services) = await promise.Task;
        experimentData.Set(MonitorTime.NewLocalPACT, (DateTime.UtcNow - time).TotalMilliseconds);
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
                experimentData.Set(MonitorTime.GenerateLocalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
            }

            // process received up level batches
            if (coordManager.batches.Count != 0)
            {
                var time = DateTime.UtcNow;
                var infos = scheduleManager.ProcessBatches(myGrainID, coordManager.batches, coordManager.higherPACTList);
                experimentData.Set(MonitorTime.ProcessRegionalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
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
                var grainID = item.Key;
                item.Value.coordID = myGrainID;
                var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);

                Immutable<Batch> immutable = new(item.Value);
                tasks.Add(grain.ReceiveBatch(immutable, commitInfo));
            }
            await Task.WhenAll(tasks);
            experimentData.Set(MonitorTime.EmitLocalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
        }
    }

    public async Task<CommitInfo> NewPACT(SnapperID primaryBid, SnapperID primaryTid, Immutable<ActorAccessInfo> actorAccessInfo)
    {
        coordManager.ReceivePACTInfo(primaryBid, primaryTid, actorAccessInfo.Value);

        var promise = new TaskCompletionSource<List<SnapperGrainID>>();
        coordManager.higherPACTList.Add(primaryTid, promise);
        await promise.Task;

        return commitInfo;
    }

    /// <summary> receive regional batch (contains multi-home / single-home multi-silo transactions) from regional coordinators </summary>
    public Task ReceiveBatch(Batch batch, CommitInfo highCommitInfo)
    {
        commitInfo.MergeCommitInfo(highCommitInfo);
        coordManager.ReceiveBatchInfo(batch);
        return Task.CompletedTask;
    }

    public async Task ACKBatchCompleteOnGrain(SnapperGrainID grainID, SnapperID primaryBid, bool isWriter)
    {
        if (isWriter) coordManager.writerGrainsPerBatch[primaryBid].Add(grainID.grainID);

        if (coordManager.numACKsPerBatch[primaryBid].Signal()) await coordManager.ReadyToCommitBatch(primaryBid);
    }

    public async Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid)
    {
        var time = DateTime.UtcNow;
        await coordManager.WaitForBatchCommit(primaryBid, bid);
        experimentData.Set(MonitorTime.CommitLocalBatch, (DateTime.UtcNow - time).TotalMilliseconds);
    }

    public Task CommitHighBatch(SnapperID primaryBid, CommitInfo highCommitInfo)
    {
        var time = DateTime.UtcNow;
        commitInfo.MergeCommitInfo(highCommitInfo);
        coordManager.CommitHighBatch(primaryBid);
        experimentData.Set(MonitorTime.CommitHighBatch, (DateTime.UtcNow - time).TotalMilliseconds);
        return Task.CompletedTask;
    }
}
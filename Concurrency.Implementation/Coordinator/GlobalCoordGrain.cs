using Utilities;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Orleans.Concurrency;
using Concurrency.Common;
using Concurrency.Interface;
using System.Diagnostics;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;

namespace Concurrency.Implementation.Coordinator;

using Schedule = Dictionary<SnapperGrainID, Batch>;

[Reentrant]
[SnapperGrainPlacementStrategy]
public class GlobalCoordGrain : Grain, IGlobalCoordGrain
{
    // coord basic info
    Guid myID;
    string myRegionID;
    SnapperGrainID myGrainID;
    CoordinatorManager coordManager;

    readonly ISnapperLoggingHelper log;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly IScheduleManager scheduleManager;

    IGlobalCoordGrain neighbor;
    Stopwatch watch;
    double batchSizeInMSecs;

    List<TaskCompletionSource<SnapperID>> actList;

    CommitInfo commitInfo;

    ExperimentData experimentData;

    public GlobalCoordGrain(ISnapperLoggingHelper log, ISnapperClusterCache snapperClusterCache, IScheduleManager scheduleManager)
    { 
        this.log = log;
        this.snapperClusterCache = snapperClusterCache;
        this.scheduleManager = scheduleManager;
    }

    public Task Init() => Task.CompletedTask;

    public Task<AggregatedExperimentData> CheckGC()
    {
        coordManager.CheckGC();
        if (actList.Count != 0) Console.WriteLine($"GlobalCoord: actList.Count = {actList.Count}. ");
        return Task.FromResult(new AggregatedExperimentData());
    }

    public override async Task OnActivateAsync(CancellationToken _)
    {
        myID = this.GetPrimaryKey();
        var strs = this.GetPrimaryKeyString().Split('+');
        myRegionID = strs[1];
        if (!strs[2].Equals(RuntimeIdentity)) return;
        myGrainID = new SnapperGrainID(myID, myRegionID + "+" + RuntimeIdentity);

        actList = new List<TaskCompletionSource<SnapperID>>();
        commitInfo = new CommitInfo();
        experimentData = new ExperimentData();

        (var _, var speculativeBatch) = snapperClusterCache.IsSpeculative();
        (var globalCoords, var regionalCoords, var localCoords) = scheduleManager.GetAllCoordInfo();
        coordManager = new CoordinatorManager(myGrainID, RuntimeIdentity, myRegionID, Hierarchy.Global, speculativeBatch, GrainFactory, log, commitInfo, experimentData, globalCoords, regionalCoords, localCoords);

        (var isFirst, var neighborGuid) = snapperClusterCache.GetNeighbor(Hierarchy.Global, myRegionID, RuntimeIdentity, myID);
        neighbor = GrainFactory.GetGrain<IGlobalCoordGrain>(neighborGuid, myRegionID + "+" + RuntimeIdentity);

        watch = new Stopwatch();
        watch.Start();
        batchSizeInMSecs = snapperClusterCache.GetBatchSize(Hierarchy.Global);
        Console.WriteLine($"GlobalCoord {myGrainID.grainID.Print()}: neighbor = {Helper.ConvertGuidToInt(neighborGuid)}, batchSize = {batchSizeInMSecs}ms");

        if (isFirst) await neighbor.PassToken();
    }

    public async Task<(SnapperID, CommitInfo)> NewACT()
    {
        var promise = new TaskCompletionSource<SnapperID>();
        actList.Add(promise);
        return (await promise.Task, commitInfo);
    }

    public async Task<(SnapperID, SnapperID, List<SnapperGrainID>, CommitInfo)> NewPACT(Immutable<ActorAccessInfo> actorAccessInfo)
    {
        var promise = new TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>();
        coordManager.pactList.Add((promise, actorAccessInfo.Value));
        (var bid, var tid, var services) = await promise.Task;
        return (bid, tid, services, commitInfo);
    }

    public async Task PassToken()
    {
        scheduleManager.UpdateCommitInfo(commitInfo);

        if (actList.Count != 0)
        {
            foreach (var act in actList) act.SetResult(scheduleManager.NewACT());
            actList.Clear();
        }

        if (watch.ElapsedMilliseconds < batchSizeInMSecs) _ = neighbor.PassToken();
        else
        {
            var schedules = new List<Schedule>();

            // generate a new local batch
            if (coordManager.pactList.Count != 0)
            {
                var info = scheduleManager.GenerateBatch(myGrainID, coordManager.pactList);
                coordManager.AddBatchMetadata(info.Item1, info.Item2, info.Item3, info.Item4);
                schedules.Add(info.Item4);
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
        var tasks = new List<Task>();
        foreach (var schedule in schedules)
        {
            var primaryBid = schedule.First().Value.GetPrimaryBid();

            if (log.DoLogging())
            {
                var grains = schedule.Keys.Select(x => new GrainID(x.grainID.id, x.grainID.className)).ToHashSet();
                await log.InfoLog(primaryBid, grains);
            }

            foreach (var item in schedule)
            {
                item.Value.coordID = myGrainID;
                var localCoord = GrainFactory.GetGrain<IRegionalCoordGrain>(item.Key.grainID.id, item.Key.location);
                tasks.Add(localCoord.ReceiveBatch(item.Value, commitInfo));
            }
        }
        await Task.WhenAll(tasks);
    }

    public async Task ACKBatchComplete(SnapperID primaryBid, HashSet<GrainID> writers)
    {
        var collectedWriters = coordManager.writerGrainsPerBatch[primaryBid];
        collectedWriters.UnionWith(writers);

        if (coordManager.numACKsPerBatch[primaryBid].Signal()) await coordManager.ReadyToCommitBatch(primaryBid);
    }

    public async Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid) => await coordManager.WaitForBatchCommit(primaryBid, bid);
}
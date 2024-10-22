using Concurrency.Interface;
using Concurrency.Common;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using Utilities;
using System.Diagnostics;
using Concurrency.Interface.Coordinator;

namespace Concurrency.Implementation;

using Schedule = Dictionary<SnapperGrainID, Batch>;

public class ScheduleManager : IScheduleManager
{
    IGrainFactory grainFactory;
    string myRegionID;
    Hierarchy myHierarchy;
    ScheduleGenerator scheduleGenerator;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly ISnapperLoggingHelper log;

    readonly CommitInfo commitInfo;

    /// <summary> the latest emitted bid of the current hierarchy </summary>
    SnapperID lastEmitBid;

    /// <summary> the latest emitted primary bid </summary>
    SnapperID lastEmitPrimaryBid;

    /// <summary> the coordinator who emitted the latest batch </summary>
    SnapperGrainID lastEmitCoordID;

    public ScheduleManager(ISnapperClusterCache snapperClusterCache, ISnapperLoggingHelper log)
    {
        this.log = log;
        lastEmitBid = new SnapperID();
        lastEmitPrimaryBid = new SnapperID();
        lastEmitCoordID = new SnapperGrainID(Guid.Empty, "");
        this.snapperClusterCache = snapperClusterCache;
        commitInfo = new CommitInfo();
    }

    public AggregatedExperimentData CheckGC() => new AggregatedExperimentData();

    public void UpdateCommitInfo(CommitInfo commitInfo)
    {
        this.commitInfo.MergeCommitInfo(commitInfo);
        commitInfo.MergeCommitInfo(this.commitInfo);
    }

    public void Init(IGrainFactory grainFactory, string myRegionID, string mySiloID, Hierarchy myHierarchy)
    {
        this.grainFactory = grainFactory;
        this.myRegionID = myRegionID;
        this.myHierarchy = myHierarchy;
        scheduleGenerator = new ScheduleGenerator(mySiloID, myRegionID, myHierarchy, snapperClusterCache, commitInfo);
        Console.WriteLine($"{myHierarchy}-ScheduleManager: {myRegionID}, {mySiloID}, is initiated");
    }

    /// <summary> get a tid for an ACT </summary>
    public SnapperID NewACT() => scheduleGenerator.GenerateNewTid();

    /// <summary> use the PACTs received so far to form a batch </summary>
    public (SnapperID, SnapperID, SnapperGrainID, Schedule) GenerateBatch(SnapperGrainID coordID, List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList)
    {
        var schedule = scheduleGenerator.GenerateBatch(pactList);

        (var prevPrimaryBid, var prevBid, var prevCoordID) = UpdateInfo(coordID, schedule.First().Value);

        return (prevPrimaryBid, prevBid, prevCoordID, schedule);
    }

    /// <summary> convert received higher level batches in specified order </summary>
    public List<(SnapperID, SnapperID, SnapperGrainID, Schedule)> ProcessBatches(
        SnapperGrainID coordID, 
        SortedDictionary<SnapperID, Batch> batches,
        Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>> higherPACTList)
    {
        var schedules = scheduleGenerator.ProcessBatches(batches, higherPACTList);

        var res = new List<(SnapperID, SnapperID, SnapperGrainID, Schedule)>();
        foreach (var schedule in schedules)
        {
            (var prevPrimaryBid, var prevBid, var prevCoordID) = UpdateInfo(coordID, schedule.First().Value);
            res.Add((prevPrimaryBid, prevBid, prevCoordID, schedule));
        }

        return res;
    }

    (SnapperID, SnapperID, SnapperGrainID) UpdateInfo(SnapperGrainID coordID, Batch batchInstance)
    {
        // =====================================================================
        // copy the old info
        var prevBid = lastEmitBid.Clone() as SnapperID;
        Debug.Assert(prevBid != null);

        var prevPrimaryBid = lastEmitPrimaryBid.Clone() as SnapperID;
        Debug.Assert(prevPrimaryBid != null);

        var prevCoordID = new SnapperGrainID(lastEmitCoordID.grainID.id, lastEmitCoordID.location);

        // =====================================================================
        // update the primary bid and coord ID
        var primaryBid = batchInstance.GetPrimaryBid();
        lastEmitPrimaryBid = primaryBid.Clone() as SnapperID;
        Debug.Assert(lastEmitPrimaryBid != null && !lastEmitPrimaryBid.isEmpty());

        SnapperID bid;
        switch (myHierarchy)
        {
            case Hierarchy.Global:
                bid = batchInstance.global_bid;
                break;
            case Hierarchy.Regional:
                bid = batchInstance.regional_bid;
                break;
            case Hierarchy.Local:
                bid = batchInstance.local_bid;
                break;
            default:
                throw new Exception($"The hierarchy {myHierarchy} is not supported. ");
        }
        lastEmitBid = bid.Clone() as SnapperID;
        Debug.Assert(lastEmitBid != null && !lastEmitBid.isEmpty());

        lastEmitCoordID = new SnapperGrainID(coordID.grainID.id, coordID.location);

        return (prevPrimaryBid, prevBid, prevCoordID);
    }

    public (Dictionary<SnapperGrainID, IGlobalCoordGrain>, Dictionary<SnapperGrainID, IRegionalCoordGrain>, Dictionary<SnapperGrainID, ILocalCoordGrain>) GetAllCoordInfo()
    {
        (var global, var regional, var local) = snapperClusterCache.GetAllCoordInfo();
        var globalCoords = new Dictionary<SnapperGrainID, IGlobalCoordGrain>();
        var regionalCoords = new Dictionary<SnapperGrainID, IRegionalCoordGrain>();
        var localCoords = new Dictionary<SnapperGrainID, ILocalCoordGrain>();
        global.ForEach(x =>
        {
            var grainID = new SnapperGrainID(x.Item3, x.Item1 + "+" + x.Item2);
            var grain = grainFactory.GetGrain<IGlobalCoordGrain>(grainID.grainID.id, grainID.location);
            globalCoords.Add(grainID, grain);
        });
        regional.ForEach(x =>
        {
            var grainID = new SnapperGrainID(x.Item3, x.Item1 + "+" + x.Item2);
            var grain = grainFactory.GetGrain<IRegionalCoordGrain>(grainID.grainID.id, grainID.location);
            regionalCoords.Add(grainID, grain);
        });
        local.ForEach(x =>
        {
            var grainID = new SnapperGrainID(x.Item3, x.Item1 + "+" + x.Item2);
            var grain = grainFactory.GetGrain<ILocalCoordGrain>(grainID.grainID.id, grainID.location);
            localCoords.Add(grainID, grain);
        });

        return (globalCoords, regionalCoords, localCoords);
    }
}
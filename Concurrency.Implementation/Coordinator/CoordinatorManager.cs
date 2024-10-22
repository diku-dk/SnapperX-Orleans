using Concurrency.Common;
using Concurrency.Interface.Coordinator;
using System.Diagnostics;
using Utilities;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Common.ILogging;

namespace Concurrency.Implementation.Coordinator;

using Schedule = Dictionary<SnapperGrainID, Batch>;

internal class CoordinatorManager
{
    readonly SnapperGrainID myCoordID;
    readonly string mySiloID;
    readonly string myRegionID;
    readonly Hierarchy myHierarchy;
    readonly IGrainFactory grainFactory;
    readonly ISnapperLoggingHelper log;
    readonly CommitInfo commitInfo;
    readonly bool speculativeBatch;
    readonly ExperimentData experimentData;

    public Dictionary<SnapperGrainID, IGlobalCoordGrain> globalCoords;

    public Dictionary<SnapperGrainID, IRegionalCoordGrain> regionalCoords;

    public Dictionary<SnapperGrainID, ILocalCoordGrain> localCoords;

    public List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList;

    // ================================================================================================== info of higher level batches
    /// <summary> primary bid, batch info </summary>
    public Dictionary<SnapperID, Batch> cachedBatches;

    /// <summary> primary bid, primary tid, access info </summary>
    public Dictionary<SnapperID, Dictionary<SnapperID, ActorAccessInfo>> cachedPACT;

    /// <summary> primary tid, the list of selected services </summary>
    public Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>> higherPACTList;

    /// <summary> batches that have received all PACT info </summary>
    public SortedDictionary<SnapperID, Batch> batches;

    // ================================================================================================== info of current generated batch
    /// <summary> primary bid </summary>
    public Dictionary<SnapperID, CountdownEvent> numACKsPerBatch;

    /// <summary> primary bid, for replication only </summary>
    public Dictionary<SnapperID, HashSet<GrainID>> writerGrainsPerBatch;

    /// <summary> primary bid </summary>
    public Dictionary<SnapperID, HashSet<SnapperGrainID>> accessedServicePerBatch;

    /// <summary> primary bid </summary>
    public Dictionary<SnapperID, TaskCompletionSource> waitForBatchCommit;

    /// <summary> primary bid, wait for higher level silo to commit this batch </summary>
    public Dictionary<SnapperID, TaskCompletionSource> waitForHighBatchCommit;

    /// <summary> primary bid, primary bid of its previous batch </summary>
    public Dictionary<SnapperID, SnapperID> primaryBidToPrevPrimaryBid;

    /// <summary> map the primary bid to the bid in the current silo </summary>
    public Dictionary<SnapperID, SnapperID> primaryBidToBid;

    /// <summary> primary bid, the coordinator who emitted this batch (in the higher level hierarchy) </summary>
    public Dictionary<SnapperID, Dictionary<Hierarchy, SnapperGrainID>> primaryBidToHighCoordID;

    // ================================================================================================== info of previous batch

    /// <summary> the previous primary bid, and the coordinator who emitted it </summary>
    public Dictionary<SnapperID, SnapperGrainID> prevPrimaryBidToCoordID;

    /// <summary> the primary bid of previous batch, and its bid in the current hierarchy </summary>
    public Dictionary<SnapperID, SnapperID> prevPrimaryBidToBid;

    public CoordinatorManager(
        SnapperGrainID coordID, string mySiloID, string myRegionID, Hierarchy myHierarchy, bool speculativeBatch, 
        IGrainFactory grainFactory, ISnapperLoggingHelper log, CommitInfo commitInfo, ExperimentData experimentData,
        Dictionary<SnapperGrainID, IGlobalCoordGrain> globalCoords,
        Dictionary<SnapperGrainID, IRegionalCoordGrain> regionalCoords,
        Dictionary<SnapperGrainID, ILocalCoordGrain> localCoords)
    {
        this.log = log;
        this.myCoordID = coordID;
        this.mySiloID = mySiloID;
        this.myRegionID = myRegionID;
        this.myHierarchy = myHierarchy;
        this.grainFactory = grainFactory;
        this.commitInfo = commitInfo;
        this.localCoords = localCoords;
        this.regionalCoords = regionalCoords;
        this.globalCoords = globalCoords;
        this.speculativeBatch = speculativeBatch;
        this.experimentData = experimentData;

        accessedServicePerBatch = new Dictionary<SnapperID, HashSet<SnapperGrainID>>();
        waitForBatchCommit = new Dictionary<SnapperID, TaskCompletionSource>();
        primaryBidToPrevPrimaryBid = new Dictionary<SnapperID, SnapperID>();
        primaryBidToHighCoordID = new Dictionary<SnapperID, Dictionary<Hierarchy, SnapperGrainID>>();
        prevPrimaryBidToCoordID = new Dictionary<SnapperID, SnapperGrainID>();
        waitForHighBatchCommit = new Dictionary<SnapperID, TaskCompletionSource>();
        primaryBidToBid = new Dictionary<SnapperID, SnapperID>();
        prevPrimaryBidToBid = new Dictionary<SnapperID, SnapperID>();
        pactList = new List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)>();
        higherPACTList = new Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>>();
        numACKsPerBatch = new Dictionary<SnapperID, CountdownEvent>();
        writerGrainsPerBatch = new Dictionary<SnapperID, HashSet<GrainID>>();
        cachedBatches = new Dictionary<SnapperID, Batch>();
        cachedPACT = new Dictionary<SnapperID, Dictionary<SnapperID, ActorAccessInfo>>();
        batches = new SortedDictionary<SnapperID, Batch>();
    }

    public void CheckGC()
    {
        if (cachedBatches.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: cachedBatches.Count = {cachedBatches.Count}");
        if (cachedPACT.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: cachedPACT.Count = {cachedPACT.Count}");
        if (batches.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: batches.Count = {batches.Count}");
        if (higherPACTList.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: higherPACTList.Count = {higherPACTList.Count}");
        if (prevPrimaryBidToBid.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: prevPrimaryBidToBid.Count = {prevPrimaryBidToBid.Count}");
        if (primaryBidToBid.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: primaryBidToBid.Count = {primaryBidToBid.Count}");
        if (prevPrimaryBidToCoordID.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: prevPrimaryBidToCoordID.Count = {prevPrimaryBidToCoordID.Count}");
        if (accessedServicePerBatch.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: accessedServicePerBatch.Count = {accessedServicePerBatch.Count}");
        if (waitForBatchCommit.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: waitForBatchCommit.Count = {waitForBatchCommit.Count}");
        if (primaryBidToPrevPrimaryBid.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: primaryBidToPrevPrimaryBid.Count = {primaryBidToPrevPrimaryBid.Count}");
        if (primaryBidToHighCoordID.Count > 1) Console.WriteLine($"{myHierarchy}-CoordinatorManager: primaryBidToHighCoordID.Count = {primaryBidToHighCoordID.Count}");
        if (waitForHighBatchCommit.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: waitForHighBatchCommit.Count = {waitForHighBatchCommit.Count}");
        if (writerGrainsPerBatch.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: writerGrainsPerBatch.Count = {writerGrainsPerBatch.Count}");
        if (numACKsPerBatch.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: numACKsPerBatch.Count = {numACKsPerBatch.Count}");
        if (pactList.Count != 0) Console.WriteLine($"{myHierarchy}-CoordinatorManager: pactList.Count = {pactList.Count}");
    }

    public void AddBatchMetadata(SnapperID prevPrimaryBid, SnapperID prevBid, SnapperGrainID prevCoordID, Schedule schedule)
    {
        var batchInstance = schedule.First().Value;
        var primaryBid = batchInstance.GetPrimaryBid();

        waitForBatchCommit.Add(primaryBid, new TaskCompletionSource());
        accessedServicePerBatch.Add(primaryBid, schedule.Keys.ToHashSet());
        primaryBidToPrevPrimaryBid.Add(primaryBid, prevPrimaryBid);
        numACKsPerBatch.Add(primaryBid, new CountdownEvent(schedule.Count));
        writerGrainsPerBatch.Add(primaryBid, new HashSet<GrainID>());

        // add coord ID info for previous 
        if (!prevPrimaryBid.isEmpty())
        {
            Debug.Assert(!prevCoordID.isEmpty());
            prevPrimaryBidToCoordID.Add(prevPrimaryBid, prevCoordID);

            if (Helper.CompareHierarchy(prevPrimaryBid.hierarchy, myHierarchy) > 0) prevPrimaryBidToBid.Add(prevPrimaryBid, prevBid);
        }
        else Debug.Assert(prevCoordID.isEmpty());

        // add coord ID info for the current batch
        if (!batchInstance.coordID.isEmpty())
        {
            var higher = Helper.GetHigherHierarchy(myHierarchy);
            primaryBidToHighCoordID.Add(primaryBid, new Dictionary<Hierarchy, SnapperGrainID>());
            primaryBidToHighCoordID[primaryBid].Add(higher, batchInstance.coordID);
            waitForHighBatchCommit.Add(primaryBid, new TaskCompletionSource());
        }

        switch (myHierarchy)
        {
            case Hierarchy.Regional:
                Debug.Assert(!batchInstance.regional_bid.isEmpty());
                primaryBidToBid.Add(primaryBid, batchInstance.regional_bid);
                break;
            case Hierarchy.Local:
                Debug.Assert(!batchInstance.local_bid.isEmpty());
                primaryBidToBid.Add(primaryBid, batchInstance.local_bid);
                break;
        }
    }

    public async Task ReadyToCommitBatch(SnapperID primaryBid)
    {
        var collectedWriters = writerGrainsPerBatch[primaryBid];
        var accessedServices = accessedServicePerBatch[primaryBid];

        var bid = primaryBidToBid.ContainsKey(primaryBid) ? primaryBidToBid[primaryBid] : primaryBid;

        var prevPrimaryBid = primaryBidToPrevPrimaryBid[primaryBid];
        var prevBid = prevPrimaryBidToBid.ContainsKey(primaryBid) ? prevPrimaryBidToBid[primaryBid] : prevPrimaryBid;

        if (speculativeBatch) await ResolveLogicalDependency(bid, primaryBid, prevBid, prevPrimaryBid, collectedWriters);
        else await ResolvePhysicalDependency(bid, primaryBid, collectedWriters);

        // now can commit the current batch
        if (primaryBid.hierarchy == myHierarchy)
        {
            if (log.DoLogging()) await log.CommitLog(primaryBid);
            if (log.DoReplication() && collectedWriters.Count != 0) await log.InfoEvent(primaryBid, collectedWriters.ToHashSet());
        }

        CommitBatch(primaryBid, prevPrimaryBid);

        var time = DateTime.UtcNow;
        var tasks = new List<Task>();
        switch (myHierarchy)
        {
            case Hierarchy.Global:
                foreach (var id in accessedServices)
                {
                    var grain = grainFactory.GetGrain<IRegionalCoordGrain>(id.grainID.id, id.location);
                    tasks.Add(grain.CommitHighBatch(primaryBid, commitInfo));
                }
                break;
            case Hierarchy.Regional:
                foreach (var id in accessedServices)
                {
                    var grain = grainFactory.GetGrain<ILocalCoordGrain>(id.grainID.id, id.location);
                    tasks.Add(grain.CommitHighBatch(primaryBid, commitInfo));
                }
                break;
            case Hierarchy.Local:
                foreach (var id in accessedServices)
                {
                    var grain = grainFactory.GetGrain<ITransactionExecutionGrain>(id.grainID.id, id.location, id.grainID.className);
                    tasks.Add(grain.CommitBatch(primaryBid, commitInfo));
                }
                break;
        }
        await Task.WhenAll(tasks);

        if (myHierarchy == Hierarchy.Regional) experimentData.Set(MonitorTime.SendBatchCommitToLocal, (DateTime.UtcNow - time).TotalMilliseconds);
        else if (myHierarchy == Hierarchy.Local) experimentData.Set(MonitorTime.SendBatchCommitToGrain, (DateTime.UtcNow - time).TotalMilliseconds);
    }

    async Task ResolvePhysicalDependency(SnapperID bid, SnapperID primaryBid, HashSet<GrainID> collectedWriters)
    {
        if (Helper.CompareHierarchy(primaryBid.hierarchy, myHierarchy) > 0)
        {
            var targetHierarchy = Helper.GetHigherHierarchy(myHierarchy);
            var highCoordID = primaryBidToHighCoordID[primaryBid][targetHierarchy];

            // send ack to higher level silo
            switch (targetHierarchy)
            {
                case Hierarchy.Global:
                    _ = globalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                    break;
                case Hierarchy.Regional:
                    _ = regionalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                    break;
                default: throw new Exception($"unsupported hierarchy {targetHierarchy}");
            }

            if (!IfBatchCommit(primaryBid, bid)) await waitForHighBatchCommit[primaryBid].Task;
        }
    }

    async Task ResolveLogicalDependency(SnapperID bid, SnapperID primaryBid, SnapperID prevBid, SnapperID prevPrimaryBid, HashSet<GrainID> collectedWriters)
    {
        if (!prevPrimaryBid.isEmpty())
        {
            var prevCoordID = prevPrimaryBidToCoordID[prevPrimaryBid];

            if (Helper.CompareHierarchy(primaryBid.hierarchy, myHierarchy) > 0)
            {
                var targetHierarchy = Helper.GetHigherHierarchy(myHierarchy);
                var highCoordID = primaryBidToHighCoordID[primaryBid][targetHierarchy];

                if (Helper.CompareHierarchy(prevPrimaryBid.hierarchy, myHierarchy) > 0)
                {
                    // send ack to higher level silo
                    switch (targetHierarchy)
                    {
                        case Hierarchy.Global:
                            _ = globalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                            break;
                        case Hierarchy.Regional:
                            _ = regionalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                            break;
                        default: throw new Exception($"unsupported hierarchy {targetHierarchy}");
                    }
                    var time = DateTime.UtcNow;
                    if (!IfBatchCommit(primaryBid, bid)) await waitForHighBatchCommit[primaryBid].Task;

                    experimentData.Set(MonitorTime.CommitBatchLocally, (DateTime.UtcNow - time).TotalMilliseconds);
                }
                else
                {
                    // wait for prev to commit in the current hierarchy level
                    if (!IfBatchCommit(prevPrimaryBid, prevBid)) await ForwardWaitForBatchCommit(prevPrimaryBid, prevBid, prevCoordID, myHierarchy);

                    // send ack and wait for the batch to be committed in the higher level silo
                    switch (targetHierarchy)
                    {
                        case Hierarchy.Global:
                            _ = globalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                            break;
                        case Hierarchy.Regional:
                            _ = regionalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                            break;
                        default: throw new Exception($"unsupported hierarchy {targetHierarchy}");
                    }

                    if (!IfBatchCommit(primaryBid, bid)) await waitForHighBatchCommit[primaryBid].Task;
                }
            }
            else
            {
                // wait for prev to commit in the current hierarchy level
                if (!IfBatchCommit(prevPrimaryBid, prevBid))
                {
                    if (prevCoordID.Equals(myCoordID)) await WaitForBatchCommit(prevPrimaryBid, prevBid);
                    else await ForwardWaitForBatchCommit(prevPrimaryBid, prevBid, prevCoordID, myHierarchy);
                }
            }
        }
        else
        {
            if (Helper.CompareHierarchy(primaryBid.hierarchy, myHierarchy) > 0)
            {
                var targetHierarchy = Helper.GetHigherHierarchy(myHierarchy);
                var highCoordID = primaryBidToHighCoordID[primaryBid][targetHierarchy];

                // send ack to higher level silo
                switch (targetHierarchy)
                {
                    case Hierarchy.Global:
                        _ = globalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                        break;
                    case Hierarchy.Regional:
                        _ = regionalCoords[highCoordID].ACKBatchComplete(primaryBid, collectedWriters);
                        break;
                    default: throw new Exception($"unsupported hierarchy {targetHierarchy}");
                }

                if (!IfBatchCommit(primaryBid, bid)) await waitForHighBatchCommit[primaryBid].Task;
            }
        }
    }

    public bool IfBatchCommit(SnapperID primaryBid, SnapperID? bid = null)
    {
        if (commitInfo.isBatchCommit(primaryBid)) return true;

        if (bid != null && commitInfo.isBatchCommit(bid)) return true;
      
        return false;
    }

    public async Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid)
    {
        if (IfBatchCommit(primaryBid, bid)) return;

        await waitForBatchCommit[primaryBid].Task;
    }

    void CommitBatch(SnapperID primaryBid, SnapperID? prevPrimaryBid = null)
    {
        commitInfo.CommitBatch(primaryBid);

        waitForBatchCommit[primaryBid].SetResult();
        waitForBatchCommit.Remove(primaryBid);

        if (primaryBidToBid.ContainsKey(primaryBid))
        {
            var bid = primaryBidToBid[primaryBid];
            commitInfo.CommitBatch(bid);
            primaryBidToBid.Remove(primaryBid);
        }

        primaryBidToPrevPrimaryBid.Remove(primaryBid);
        primaryBidToHighCoordID.Remove(primaryBid);
        accessedServicePerBatch.Remove(primaryBid);
        numACKsPerBatch.Remove(primaryBid);
        writerGrainsPerBatch.Remove(primaryBid);

        if (prevPrimaryBid != null)
        {
            prevPrimaryBidToCoordID.Remove(prevPrimaryBid);
            prevPrimaryBidToBid.Remove(prevPrimaryBid);
        }
    }

    public void CommitHighBatch(SnapperID primaryBid)
    {
        commitInfo.CommitBatch(primaryBid);
        waitForHighBatchCommit[primaryBid].SetResult();
        waitForHighBatchCommit.Remove(primaryBid);
    }

    async Task ForwardWaitForBatchCommit(SnapperID primaryBid, SnapperID bid, SnapperGrainID coordID, Hierarchy targetHierarchy)
    {
        switch (targetHierarchy)
        {
            case Hierarchy.Global:
                await globalCoords[coordID].WaitForBatchCommit(primaryBid, bid);
                break;
            case Hierarchy.Regional:
                await regionalCoords[coordID].WaitForBatchCommit(primaryBid, bid);
                break;
            case Hierarchy.Local:
                await localCoords[coordID].WaitForBatchCommit(primaryBid, bid);
                break;
        }
    }

    public void ReceiveBatchInfo(Batch batch)
    {
        var primaryBid = batch.GetPrimaryBid();
        cachedBatches.Add(primaryBid, batch);

        CheckBatchInfo(primaryBid);
    }

    public void ReceivePACTInfo(SnapperID primaryBid, SnapperID primaryTid, ActorAccessInfo actorAccessInfo)
    {
        if (!cachedPACT.ContainsKey(primaryBid)) cachedPACT.Add(primaryBid, new Dictionary<SnapperID, ActorAccessInfo>());

        Debug.Assert(!cachedPACT[primaryBid].ContainsKey(primaryTid));
        cachedPACT[primaryBid].Add(primaryTid, actorAccessInfo);

        CheckBatchInfo(primaryBid);
    }

    void CheckBatchInfo(SnapperID primaryBid)
    {
        if (!cachedPACT.ContainsKey(primaryBid) || !cachedBatches.ContainsKey(primaryBid)) return;

        var batch = cachedBatches[primaryBid];

        var numExpectedTxnInfo = batch.txnList.Count;
        var numActualTxnInfo = cachedPACT[primaryBid].Count;

        if (numActualTxnInfo == numExpectedTxnInfo)
        {
            var newTxnList = new List<(SnapperID, ActorAccessInfo)>();
            foreach (var txn in batch.txnList)
            {
                var primaryTid = txn.Item1;
                newTxnList.Add((primaryTid, cachedPACT[primaryBid][primaryTid]));
            }

            batch.txnList = newTxnList;

            cachedBatches.Remove(primaryBid);
            cachedPACT.Remove(primaryBid);

            switch (myHierarchy)
            {
                case Hierarchy.Regional:
                    batches.Add(batch.global_bid, batch);
                    break;
                case Hierarchy.Local:
                    batches.Add(batch.regional_bid, batch);
                    break;
            }
        }
    }
}
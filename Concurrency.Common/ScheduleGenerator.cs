using System.Diagnostics;
using Utilities;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Amazon.Runtime.Internal.Transform;

namespace Concurrency.Common;

using Schedule = Dictionary<SnapperGrainID, Batch>;

public class ScheduleGenerator
{
    readonly string mySiloID;
    readonly string myRegionID;
    readonly Hierarchy myHierarchy;
    readonly ISnapperCache snapperCache;

    readonly CommitInfo commitInfo;

    long lastEmitTid;
    long lastEmitBid;

    /// <summary> For regional coordinator, the up level bid is global bid. For local coordinator: the up level bid is regional bid </summary>
    SnapperID lastProcessedUpLevelBid;

    /// <summary> <regionID / siloID / grainID, last batch emitted to this service> </summary>
    Dictionary<string, SnapperID> lastBidPerService;

    /// <summary> grain ID, last primary bid emitted to this grain </summary>
    Dictionary<string, SnapperID> lastPrimaryBidPerService;

    /// <summary> grain ID, key, last primary bid, last primary tid </summary>
    Dictionary<SnapperGrainID, Dictionary<ISnapperKey, (SnapperID, SnapperID)>> lastPACTPerKey;

    public ScheduleGenerator(string mySiloID, string myRegionID, Hierarchy myHierarchy, ISnapperCache snapperCache, CommitInfo commitInfo)
    { 
        this.mySiloID = mySiloID;
        this.myRegionID = myRegionID;
        this.myHierarchy = myHierarchy;
        this.snapperCache = snapperCache;
        this.commitInfo = commitInfo;

        lastEmitTid = -1;
        lastEmitBid = -1;
        lastProcessedUpLevelBid = new SnapperID();
        lastBidPerService = new Dictionary<string, SnapperID>();
        lastPrimaryBidPerService = new Dictionary<string, SnapperID>();
        lastPACTPerKey = new Dictionary<SnapperGrainID, Dictionary<ISnapperKey, (SnapperID, SnapperID)>>();
    }

    public SnapperID GenerateNewTid() => new SnapperID(++lastEmitTid, mySiloID, myRegionID, myHierarchy);

    public Schedule GenerateBatch(List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList)
    {
        // assign bid and tid for transactions
        var schedule = new Schedule();
        var currentBid = ++lastEmitBid;
        var bid = new SnapperID(currentBid, mySiloID, myRegionID, myHierarchy);
        
        var selectedTargetService = new Dictionary<string, SnapperGrainID>();

        foreach (var txn in pactList)
        {
            var selectedServicesForTxn = new Dictionary<string, SnapperGrainID>();

            var txnID = GenerateNewTid();

            var promise = txn.Item1;
            var actorAccessInfo = txn.Item2;

            // generate batch schedule
            List<string> regionIDs;
            List<string> siloIDs;
            HashSet<SnapperGrainID> grainIDs;
            switch (myHierarchy)
            {
                case Hierarchy.Global:
                    regionIDs = actorAccessInfo.GetAccessRegions();
                    Debug.Assert(regionIDs.Count > 1);
                    foreach (var regionID in regionIDs)
                    {
                        if (!selectedTargetService.ContainsKey(regionID))
                        {
                            var guid = snapperCache.GetOneRandomRegionalCoord(regionID);
                            var siloID = snapperCache.GetRegionalSiloID(regionID);
                            var regionalCoordID = new SnapperGrainID(guid, regionID + "+" + siloID);
                            selectedTargetService.Add(regionID, regionalCoordID);
                        }

                        if (!selectedServicesForTxn.ContainsKey(regionID)) selectedServicesForTxn.Add(regionID, selectedTargetService[regionID]);

                        GnerateSchedulePerService(new Batch(), bid, regionID, selectedTargetService[regionID], schedule, (txnID, new ActorAccessInfo()));
                    }
                    break;
                case Hierarchy.Regional:
                    regionIDs = actorAccessInfo.GetAccessRegions();
                    siloIDs = actorAccessInfo.GetAccssSilos(myRegionID);
                    Debug.Assert(regionIDs.Count == 1 && regionIDs.First() == myRegionID);
                    Debug.Assert(siloIDs.Count > 1);
                    foreach (var siloID in siloIDs)
                    {
                        if (!selectedTargetService.ContainsKey(siloID))
                        {
                            var guid = snapperCache.GetOneRandomLocalCoord(myRegionID, siloID);
                            var localCoordID = new SnapperGrainID(guid, myRegionID + "+" + siloID);
                            selectedTargetService.Add(siloID, localCoordID);
                        }

                        if (!selectedServicesForTxn.ContainsKey(siloID)) selectedServicesForTxn.Add(siloID, selectedTargetService[siloID]);

                        GnerateSchedulePerService(new Batch(), bid, siloID, selectedTargetService[siloID], schedule, (txnID, new ActorAccessInfo()));
                    }
                    break;
                case Hierarchy.Local:
                    regionIDs = actorAccessInfo.GetAccessRegions();
                    siloIDs = actorAccessInfo.GetAccssSilos(myRegionID);
                    grainIDs = actorAccessInfo.GetAccessGrains(myRegionID, mySiloID);
                    Debug.Assert(regionIDs.Count == 1 && regionIDs.First() == myRegionID);
                    Debug.Assert(siloIDs.Count == 1 && siloIDs.First() == mySiloID);
                    foreach (var grainID in grainIDs)
                    {
                        var info = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
                        info.Add(regionIDs.First(), new Dictionary<string, HashSet<SnapperGrainID>>());
                        info[regionIDs.First()].Add(siloIDs.First(), new HashSet<SnapperGrainID> { grainID });

                        var keysPerGrain = actorAccessInfo.keyToAccessPerGrain.Where(x => x.Key.Equals(grainID)).ToDictionary(x => x.Key, x => x.Value);
                        var accessInfo = new ActorAccessInfo(info, keysPerGrain);
                        GnerateSchedulePerService(new Batch(), bid, grainID.ToString(), grainID, schedule, (txnID, accessInfo));
                    }
                    break;
            }

            promise.SetResult((bid, txnID, selectedServicesForTxn.Values.ToList()));
        }

        pactList.Clear();

        return schedule;
    }

    public List<Schedule> ProcessBatches(
        SortedDictionary<SnapperID, Batch> batches,
        Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>> higherPACTList)
    {
        Debug.Assert(myHierarchy != Hierarchy.Global);
        var schedules = new List<Schedule>();

        while (batches.Count != 0)
        {
            var batch = batches.First();
            Debug.Assert(batch.Value.coordID != null);
            
            // if this is regional silo, it is rpoessing global batches now
            if (myHierarchy == Hierarchy.Regional && !batch.Value.global_prevBid.Equals(lastProcessedUpLevelBid)) break;

            // if this is local silo, it is processing regional batches now
            if (myHierarchy == Hierarchy.Local && !batch.Value.regional_prevBid.Equals(lastProcessedUpLevelBid)) break;

            var schedule = new Schedule();
            schedules.Add(schedule);
            var currentBid = ++lastEmitBid;
            var bid = new SnapperID(currentBid, mySiloID, myRegionID, myHierarchy);

            Debug.Assert(!lastProcessedUpLevelBid.Equals(batch.Key));
            lastProcessedUpLevelBid = batch.Key;

            var txnList = batch.Value.txnList;
            Debug.Assert(txnList.Count != 0);
            batch.Value.txnList = new List<(SnapperID, ActorAccessInfo)>();
            var selectedTargetService = new Dictionary<string, SnapperGrainID>();
            foreach (var txnInfo in txnList)
            {
                var tid = txnInfo.Item1;
                var actorAccessInfo = txnInfo.Item2;
                
                // generate batch schedule
                List<string> siloIDs;
                HashSet<SnapperGrainID> grainIDs;
                switch (myHierarchy)
                {
                    case Hierarchy.Regional:
                        siloIDs = actorAccessInfo.GetAccssSilos(myRegionID);
                        foreach (var siloID in siloIDs)
                        {
                            if (!selectedTargetService.ContainsKey(siloID))
                            {
                                var guid = snapperCache.GetOneRandomLocalCoord(myRegionID, siloID);
                                var localCoordID = new SnapperGrainID(guid, myRegionID + "+" + siloID);
                                selectedTargetService.Add(siloID, localCoordID);
                            }

                            GnerateSchedulePerService(batch.Value, bid, siloID, selectedTargetService[siloID], schedule, (tid, new ActorAccessInfo()));
                        }
                        break;
                    case Hierarchy.Local:
                        grainIDs = actorAccessInfo.GetAccessGrains(myRegionID, mySiloID);
                        foreach (var grainID in grainIDs)
                        {
                            var info = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
                            info.Add(myRegionID, new Dictionary<string, HashSet<SnapperGrainID>>());
                            info[myRegionID].Add(mySiloID, new HashSet<SnapperGrainID> { grainID });

                            var keysPerGrain = actorAccessInfo.keyToAccessPerGrain.Where(x => x.Key.Equals(grainID)).ToDictionary(x => x.Key, x => x.Value);
                            var accessInfo = new ActorAccessInfo(info, keysPerGrain);
                            GnerateSchedulePerService(batch.Value, bid, grainID.ToString(), grainID, schedule, (tid, accessInfo));
                        }
                        break;
                }

                higherPACTList[txnInfo.Item1].SetResult(selectedTargetService.Values.ToList());
                higherPACTList.Remove(txnInfo.Item1);
            }

            var removed = batches.Remove(batch.Key);
            Debug.Assert(removed);
        }

        return schedules;
    }

    void GnerateSchedulePerService(
        Batch originalBatch,
        SnapperID currentBid,
        string serviceID,                // region ID,         silo ID,        grain ID
        SnapperGrainID targetServiceID,  // regional coord ID, local coord ID, grain ID
        Schedule schedule,
        (SnapperID, ActorAccessInfo) txnInfo)
    {
        var batch = (Batch)originalBatch.Clone();
        if (!schedule.ContainsKey(targetServiceID))
        {
            var lastBid = lastBidPerService.ContainsKey(serviceID) ? lastBidPerService[serviceID] : new SnapperID();
            batch.SetSnapperID(currentBid, lastBid, myHierarchy);
            schedule.Add(targetServiceID, batch);
            lastBidPerService[serviceID] = currentBid;

            if (myHierarchy == Hierarchy.Local)
            {
                var lastPrimaryBid = lastPrimaryBidPerService.ContainsKey(serviceID) ? lastPrimaryBidPerService[serviceID] : new SnapperID();
                batch.SetPrevLocalPrimaryBid(lastPrimaryBid);

                var primaryBid = batch.GetPrimaryBid();
                Debug.Assert(!primaryBid.isEmpty());
                lastPrimaryBidPerService[serviceID] = primaryBid;
            }
        }
        
        batch = schedule[targetServiceID];
        if (myHierarchy == Hierarchy.Local)
        {
            if (txnInfo.Item2.keyToAccessPerGrain.Count != 0)
            {
                var keys = txnInfo.Item2.keyToAccessPerGrain[targetServiceID];
                Debug.Assert(keys.Count != 0);

                if (!lastPACTPerKey.ContainsKey(targetServiceID)) lastPACTPerKey.Add(targetServiceID, new Dictionary<ISnapperKey, (SnapperID, SnapperID)>());

                var dictionary = lastPACTPerKey[targetServiceID];
                foreach (var key in keys)
                {
                    var bid = batch.GetPrimaryBid();
                    var tid = txnInfo.Item1;
                    Debug.Assert(bid != null && tid != null);

                    if (dictionary.ContainsKey(key))
                    {
                        (var depBid, var depTid) = dictionary[key];
                        if (!commitInfo.isBatchCommit(depBid)) batch.AddTxnDependencyOnKey(key, tid, depBid, depTid);
                    }

                    dictionary[key] = (bid, tid);
                }
            }

            schedule[targetServiceID].AddTxn((txnInfo.Item1, new ActorAccessInfo()));
        }
        else schedule[targetServiceID].AddTxn(txnInfo);
    }
}
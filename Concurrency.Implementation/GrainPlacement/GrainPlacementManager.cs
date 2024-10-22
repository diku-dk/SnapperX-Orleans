using Orleans.Concurrency;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.Coordinator;
using Concurrency.Common;
using Concurrency.Interface.TransactionExecution;
using Utilities;
using Concurrency.Interface;
using System.Diagnostics;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Concurrency.Interface.DataModel;

namespace Concurrency.Implementation.GrainPlacement;

[Reentrant]
[SnapperGrainPlacementStrategy]
public class GrainPlacementManager : Grain, IGrainPlacementManager
{
    SnapperGrainID myID;
    string myRegionID;
    string mySiloID;
    IGlobalCoordGrain globalCoordGrain;
    Dictionary<string,  IRegionalCoordGrain> regionalCoordGrains;
    Dictionary<string, Dictionary<string, ILocalCoordGrain>> localCoordGrains;
    Dictionary<string, Dictionary<string, IGrainPlacementManager>> pmPerRegionPerSilo;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly IScheduleManager scheduleManager;
    CommitInfo commitInfo;

    // for grain migration
    Dictionary<GrainID, TaskCompletionSource> freezedGrains;                                  // <grain ID, when the grain is unfreezed>
    Dictionary<GrainID, MyCounter> numToBeRegisteredTxnPerGrain;                              // <grain ID, number of transactions that will be registered but haven't got txn context>
    Dictionary<GrainID, TaskCompletionSource> allTxnRegisteredPerGrain;                       // <grain ID, when all transactions have either got local tid or global tid>
    Dictionary<GrainID, (SnapperID, TaskCompletionSource)> maxUnCommittedLocalBidPerGrain;    // <grain ID, max local bid, the task is set when the max bid is committed>
    Dictionary<GrainID, (SnapperID, TaskCompletionSource)> maxUnCommittedRegionalBidPerGrain;
    Dictionary<GrainID, (SnapperID, TaskCompletionSource)> maxUnCommittedGlobalBidPerGrain;
                
    public GrainPlacementManager(ISnapperClusterCache snapperClusterCache, IScheduleManager scheduleManager)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.scheduleManager = scheduleManager;
    }

    public Task<AggregatedExperimentData> CheckGC()
    {
        if (freezedGrains.Count != 0) Console.WriteLine($"PM: freezedGrains.Count = {freezedGrains.Count}");
        if (numToBeRegisteredTxnPerGrain.Count != 0) Console.WriteLine($"PM: numToBeRegisteredTxnPerGrain.Count = {numToBeRegisteredTxnPerGrain.Count}");
        if (allTxnRegisteredPerGrain.Count != 0) Console.WriteLine($"PM: allTxnRegisteredPerGrain.Count = {allTxnRegisteredPerGrain.Count}");
        if (maxUnCommittedLocalBidPerGrain.Count != 0) Console.WriteLine($"PM: maxUnCommittedLocalBidPerGrain.Count = {maxUnCommittedLocalBidPerGrain.Count}");
        if (maxUnCommittedRegionalBidPerGrain.Count != 0) Console.WriteLine($"PM: maxUnCommittedRegionalBidPerGrain.Count = {maxUnCommittedRegionalBidPerGrain.Count}");
        if (maxUnCommittedGlobalBidPerGrain.Count != 0) Console.WriteLine($"PM: maxUnCommittedGlobalBidPerGrain.Count = {maxUnCommittedGlobalBidPerGrain.Count}");

        return Task.FromResult(new AggregatedExperimentData());
    }

    public override Task OnActivateAsync(CancellationToken _)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(GrainPlacementManager).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        // randomly select a global coordinator
        var globalCoordID = snapperClusterCache.GetOneRandomGlobalCoord();
        (var globalRegion, var globalSilo) = snapperClusterCache.GetGlobalRegionAndSilo();
        if (globalRegion != myRegionID)
        {
            var globalClient = snapperClusterCache.GetOrleansClient(globalRegion);
            globalCoordGrain = globalClient.GetGrain<IGlobalCoordGrain>(globalCoordID, globalRegion + "+" + globalSilo);
        }
        else globalCoordGrain = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID, globalRegion + "+" + globalSilo);

        // randomly select a regional coordinator for each region
        regionalCoordGrains = new Dictionary<string, IRegionalCoordGrain>();
        var regionList = snapperClusterCache.GetRegionList();
        foreach (var regionID in regionList)
        {
            var regionalCoordID = snapperClusterCache.GetOneRandomRegionalCoord(regionID);
            var regionalSiloID = snapperClusterCache.GetRegionalSiloID(regionID);
            if (regionID != myRegionID)
            {
                var regionalClient = snapperClusterCache.GetOrleansClient(regionID);
                var regionalCoordGrain = regionalClient.GetGrain<IRegionalCoordGrain>(regionalCoordID, regionID + "+" + regionalSiloID);
                regionalCoordGrains.Add(regionID, regionalCoordGrain);
            }
            else
            {
                var regionalCoordGrain = GrainFactory.GetGrain<IRegionalCoordGrain>(regionalCoordID, regionID + "+" + regionalSiloID);
                regionalCoordGrains.Add(regionID, regionalCoordGrain);
            }
        }

        // randomly select a local coordinator for each local silo in each region
        localCoordGrains = new Dictionary<string, Dictionary<string, ILocalCoordGrain>>();
        foreach (var regionID in regionList)
        {
            localCoordGrains.Add(regionID, new Dictionary<string, ILocalCoordGrain>());
            var siloList = snapperClusterCache.GetLocalSiloList(regionID);
            foreach (var siloID in siloList)
            {
                var localCoordID = snapperClusterCache.GetOneRandomLocalCoord(regionID, siloID);
                ILocalCoordGrain localCoordGrain;
                if (regionID != myRegionID)
                {
                    var regionalClient = snapperClusterCache.GetOrleansClient(regionID);
                    localCoordGrain = regionalClient.GetGrain<ILocalCoordGrain>(localCoordID, regionID + "+" + siloID);
                }
                else localCoordGrain = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID, regionID + "+" + siloID);
                localCoordGrains[regionID].Add(siloID, localCoordGrain);
            }
        }

        pmPerRegionPerSilo = new Dictionary<string, Dictionary<string, IGrainPlacementManager>>();
        foreach (var regionID in regionList)
        {
            pmPerRegionPerSilo.Add(regionID, new Dictionary<string, IGrainPlacementManager>());
            var siloList = snapperClusterCache.GetLocalSiloList(regionID);
            foreach (var siloID in siloList)
            {
                var pmID = snapperClusterCache.GetOneRandomPlacementManager(regionID, siloID);
                IGrainPlacementManager pmGrain;
                if (regionID != myRegionID)
                {
                    var regionalClient = snapperClusterCache.GetOrleansClient(regionID);
                    pmGrain = regionalClient.GetGrain<IGrainPlacementManager>(pmID, regionID + "+" + siloID);
                }
                else pmGrain = GrainFactory.GetGrain<IGrainPlacementManager>(pmID, regionID + "+" + siloID);
                pmPerRegionPerSilo[regionID].Add(siloID, pmGrain);
            }
        }

        freezedGrains = new Dictionary<GrainID, TaskCompletionSource>();
        numToBeRegisteredTxnPerGrain = new Dictionary<GrainID, MyCounter>();
        allTxnRegisteredPerGrain = new Dictionary<GrainID, TaskCompletionSource>();
        maxUnCommittedLocalBidPerGrain = new Dictionary<GrainID, (SnapperID, TaskCompletionSource)>();
        maxUnCommittedRegionalBidPerGrain = new Dictionary<GrainID, (SnapperID, TaskCompletionSource)>();
        maxUnCommittedGlobalBidPerGrain = new Dictionary<GrainID, (SnapperID, TaskCompletionSource)>();

        commitInfo = new CommitInfo();

        return Task.CompletedTask;
    }

    public async Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc, List<GrainID> grains, Dictionary<GrainID, HashSet<ISnapperKey>>? keysToAccessPerGrain = null)
    {
        // check if need to forward the transaction to another PM
        var masterGrain = snapperClusterCache.GetMasterGrain(startGrainID);
        (var masterRegion, var masterSilo) = masterGrain.GetLocation();
        if (masterRegion != myRegionID || masterSilo != mySiloID)
        {
            //Console.WriteLine($"PM: forward PACT reqeust to another PM in region {masterRegion}, silo {masterSilo}");
            var pm = pmPerRegionPerSilo[masterRegion][masterSilo];
            return await pm.SubmitTransaction(startGrainID, startFunc, grains, keysToAccessPerGrain);
        }

        // get the transaction context
        var time = DateTime.UtcNow;
        var result = new TransactionResult();

        // check if any grain is already freezed
        foreach (var id in grains)
        {
            if (freezedGrains.ContainsKey(id))
            {
                result.SetException(ExceptionType.GrainMigration);
                return result;
            }
        }
            
        foreach (var id in grains)
        {
            if (!numToBeRegisteredTxnPerGrain.ContainsKey(id)) numToBeRegisteredTxnPerGrain.Add(id, new MyCounter());
            numToBeRegisteredTxnPerGrain[id].Add(1);
        }

        var cxt = await NewPACT(grains, keysToAccessPerGrain);

        // mark that each grain involved in the transaction has got its context
        foreach (var id in grains)
        {
            numToBeRegisteredTxnPerGrain[id].Deduct(1);
            if (numToBeRegisteredTxnPerGrain[id].Get() == 0)
            {
                if (allTxnRegisteredPerGrain.ContainsKey(id))
                {
                    allTxnRegisteredPerGrain[id].SetResult();
                    allTxnRegisteredPerGrain.Remove(id);
                }

                numToBeRegisteredTxnPerGrain.Remove(id);
            }
        }

        var registerTime = (DateTime.UtcNow - time).TotalMilliseconds;
        time = DateTime.UtcNow;

        // invoke call on the first grain
        var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(masterGrain.grainID.id, masterGrain.location, masterGrain.grainID.className);

        result = await grain.ExecutePACT(startFunc, cxt, time);
        result.time.Add(BreakDownLatency.REGISTER, registerTime);

        commitInfo.CommitBatch(cxt.bid);

        // do garbage collection based on latest commit info
        var itemsToRemove = maxUnCommittedLocalBidPerGrain.Where(x => commitInfo.isBatchCommit(x.Value.Item1)).Select(x => x.Key).ToList();
        foreach (var item in itemsToRemove)
        {
            maxUnCommittedLocalBidPerGrain[item].Item2.SetResult();
            maxUnCommittedLocalBidPerGrain.Remove(item);
        }

        itemsToRemove = maxUnCommittedRegionalBidPerGrain.Where(x => commitInfo.isBatchCommit(x.Value.Item1)).Select(x => x.Key).ToList();
        foreach (var item in itemsToRemove)
        {
            maxUnCommittedRegionalBidPerGrain[item].Item2.SetResult();
            maxUnCommittedRegionalBidPerGrain.Remove(item);
        }

        itemsToRemove = maxUnCommittedGlobalBidPerGrain.Where(x => commitInfo.isBatchCommit(x.Value.Item1)).Select(x => x.Key).ToList();
        foreach (var item in itemsToRemove)
        {
            maxUnCommittedGlobalBidPerGrain[item].Item2.SetResult();
            maxUnCommittedGlobalBidPerGrain.Remove(item);
        }

        return result;
    }

    public async Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc)
    {
        if (freezedGrains.ContainsKey(startGrainID))
        {
            var res = new TransactionResult();
            res.SetException(ExceptionType.GrainMigration);
            return res;
        }
        
        var masterGrain = snapperClusterCache.GetMasterGrain(startGrainID);
        (var masterRegion, var masterSilo) = masterGrain.GetLocation();
        if (masterRegion != myRegionID || masterSilo != mySiloID)
        {
            //Console.WriteLine($"PM: forward ACT reqeust to another PM in region {masterRegion}, silo {masterSilo}");
            var pm = pmPerRegionPerSilo[masterRegion][masterSilo];
            return await pm.SubmitTransaction(startGrainID, startFunc);
        }
        else
        {
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(masterGrain.grainID.id, masterGrain.location, masterGrain.grainID.className);
            
            try
            {
                return await grain.ExecuteACT(startFunc);
            }
            catch (Exception e)
            {
                Console.WriteLine($"SubmitTransaction (ACT): {masterGrain.grainID.Print()}, {startFunc.funcName}, {e.Message}, {e.StackTrace}");
                Debug.Assert(false);
                throw;
            }
        }
    }

    public async Task<TransactionResult> SubmitNonTransactionalRequest(GrainID startGrainID, FunctionCall startFunc)
    {
        var masterGrain = snapperClusterCache.GetMasterGrain(startGrainID);
        (var masterRegion, var masterSilo) = masterGrain.GetLocation();
        if (masterRegion != myRegionID || masterSilo != mySiloID)
        {
            //Console.WriteLine($"PM: forward ACT reqeust to another PM in region {masterRegion}, silo {masterSilo}");
            var pm = pmPerRegionPerSilo[masterRegion][masterSilo];
            return await pm.SubmitNonTransactionalRequest(startGrainID, startFunc);
        }
        else
        {
            var txnResult = new TransactionResult();

            var grain = GrainFactory.GetGrain<INonTransactionalKeyValueGrain>(masterGrain.grainID.id, masterGrain.location, masterGrain.grainID.className);

            try
            {
                (var tid, var newCommitInfo) = await globalCoordGrain.NewACT();
                commitInfo.MergeCommitInfo(newCommitInfo);
                var funcResult = await grain.Execute(startFunc, tid);
                txnResult.MergeResult(funcResult);
                txnResult.resultObj = funcResult.resultObj;
            }
            catch (Exception e)
            {
                txnResult.SetException(ExceptionType.AppAbort);
                //Console.WriteLine($"SubmitNonTransactionalRequest: {masterGrain.grainID.Print()}, {startFunc.funcName}, {e.Message}, {e.StackTrace}");
                //Debug.Assert(false);
            }

            return txnResult;
        }
    }

    public async Task<TransactionContext> NewPACT(List<GrainID> grains, Dictionary<GrainID, HashSet<ISnapperKey>>? keysToAccessPerGrain = null)
    {
        var txnInfo = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();  // region ID, silo ID, grain ID
        var txnKeyAccessInfo = new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>();
        foreach (var grain in grains)
        {
            var masterGrainID = snapperClusterCache.GetMasterGrain(grain);
            (var regionID, var siloID) = masterGrainID.GetLocation();
            if (!txnInfo.ContainsKey(regionID)) txnInfo.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());
            if (!txnInfo[regionID].ContainsKey(siloID)) txnInfo[regionID].Add(siloID, new HashSet<SnapperGrainID>());

            txnInfo[regionID][siloID].Add(new SnapperGrainID(grain.id, regionID + "+" + siloID, grain.className));

            if (keysToAccessPerGrain != null)
                if (keysToAccessPerGrain.ContainsKey(grain)) txnKeyAccessInfo.Add(masterGrainID, keysToAccessPerGrain[grain]);
        }
        var actorAccessInfo = new ActorAccessInfo(txnInfo, txnKeyAccessInfo);
        
        SnapperID primaryBid;
        SnapperID primaryTid;
        var regionList = actorAccessInfo.GetAccessRegions();

        if (regionList.Count > 1)
        {
            // ================ global =================================================================================================================
            (primaryBid, primaryTid, var selectedRegionalCoords) = await GetGlobalContext(grains, regionList);

            foreach (var regionalCoordID in selectedRegionalCoords)
            {
                var selectedLocalCoords = await ForwardToRegionalCoord(primaryBid, primaryTid, regionalCoordID, actorAccessInfo);

                var tasks = new List<Task>();
                foreach (var localCoordID in selectedLocalCoords) tasks.Add(ForwardToLocalCoord(primaryBid, primaryTid, localCoordID, actorAccessInfo));
                await Task.WhenAll(tasks);
            }
        }
        else
        {
            var regionID = regionList.First();
            var siloList = actorAccessInfo.GetAccssSilos(regionList.First());
            if (siloList.Count > 1)
            {
                // ================ regional ===========================================================================================================
                (primaryBid, primaryTid, var selectedLocalCoords) = await GetRegionalContext(grains, regionID, siloList);
                
                var tasks = new List<Task>();
                foreach (var localCoordID in selectedLocalCoords) tasks.Add(ForwardToLocalCoord(primaryBid, primaryTid, localCoordID, actorAccessInfo));
                await Task.WhenAll(tasks);
            }
            else
            {
                // ================ local ===============================================================================================================
                var siloID = siloList.First();
                (primaryBid, primaryTid) = await GetLocalContext(regionID, siloID, actorAccessInfo);
            }
        }

        UpdateInfo(grains, primaryBid);
        return new TransactionContext(primaryBid, primaryTid);
    }

    async Task<List<SnapperGrainID>> ForwardToRegionalCoord(SnapperID primaryBid, SnapperID primaryTid, SnapperGrainID regionalCoordID, ActorAccessInfo actorAccessInfo)
    {
        (var regionID, _) = regionalCoordID.GetLocation();

        var regionalInfo = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        regionalInfo.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());

        var siloList = actorAccessInfo.info[regionID].Keys.ToList();
        foreach (var siloID in siloList) regionalInfo[regionID].Add(siloID, new HashSet<SnapperGrainID>());
        var regionalAccessInfo = new ActorAccessInfo(regionalInfo, new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>());

        var regionalCoord = GrainFactory.GetGrain<IRegionalCoordGrain>(regionalCoordID.grainID.id, regionalCoordID.location);

        Immutable<ActorAccessInfo> immutable = new(regionalAccessInfo);
        (var selectedLocalCoords, var newCommitInfo) = await regionalCoord.NewPACT(primaryBid, primaryTid, immutable);

        commitInfo.MergeCommitInfo(newCommitInfo);

        return selectedLocalCoords;
    }

    async Task ForwardToLocalCoord(SnapperID primaryBid, SnapperID primaryTid, SnapperGrainID localCoordID, ActorAccessInfo actorAccessInfo)
    {
        (var regionID, var siloID) = localCoordID.GetLocation();

        var localInfo = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        localInfo.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());
        var grains = actorAccessInfo.info[regionID][siloID];
        localInfo[regionID].Add(siloID, grains);
        var keysPerGrain = actorAccessInfo.keyToAccessPerGrain.Where(x => grains.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value);
        var localAccessInfo = new ActorAccessInfo(localInfo, keysPerGrain);

        var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID.grainID.id, localCoordID.location);

        Immutable<ActorAccessInfo> immutable = new(localAccessInfo);
        var newCommitInfo = await localCoord.NewPACT(primaryBid, primaryTid, immutable);

        commitInfo.MergeCommitInfo(newCommitInfo);
    }

    async Task<(SnapperID, SnapperID, List<SnapperGrainID>)> GetGlobalContext(List<GrainID> grains, List<string> regionList)
    {
        var globalInfo = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        foreach (var regionID in regionList) globalInfo.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());
        var globalAccessInfo = new ActorAccessInfo(globalInfo, new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>());

        Immutable<ActorAccessInfo> immutable = new(globalAccessInfo);
        (var bid, var tid, var selectedRegionalCoords, var newCommitInfo) = await globalCoordGrain.NewPACT(immutable);

        commitInfo.MergeCommitInfo(newCommitInfo);

        return (bid, tid, selectedRegionalCoords);
    }

    async Task<(SnapperID, SnapperID, List<SnapperGrainID>)> GetRegionalContext(List<GrainID> grains, string regionID, List<string> siloList)
    {
        var regionalInfo = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        regionalInfo.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());
        foreach (var siloID in siloList) regionalInfo[regionID].Add(siloID, new HashSet<SnapperGrainID>());
        var regionalAccessInfo = new ActorAccessInfo(regionalInfo, new Dictionary<SnapperGrainID, HashSet<ISnapperKey>>());

        Immutable<ActorAccessInfo> immutable = new(regionalAccessInfo);
        (var bid, var tid, var selectedLocalCoords, var newCommitInfo) = await regionalCoordGrains[regionID].NewPACT(immutable);

        commitInfo.MergeCommitInfo(newCommitInfo);

        return (bid, tid, selectedLocalCoords);
    }

    async Task<(SnapperID, SnapperID)> GetLocalContext(string regionID, string siloID, ActorAccessInfo actorAccessInfo)
    {
        var grains = actorAccessInfo.info[regionID][siloID];

        var localInfo = new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>>();
        localInfo.Add(regionID, new Dictionary<string, HashSet<SnapperGrainID>>());
        localInfo[regionID].Add(siloID, grains);

        var keysPerGrain = actorAccessInfo.keyToAccessPerGrain.Where(x => grains.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value);
        var localAccessInfo = new ActorAccessInfo(localInfo, keysPerGrain);

        Immutable<ActorAccessInfo> immutable = new(localAccessInfo);
        (var bid, var tid, _, var newCommitInfo) = await localCoordGrains[regionID][siloID].NewPACT(immutable);

        commitInfo.MergeCommitInfo(newCommitInfo);

        return (bid, tid);
    }

    void UpdateInfo(List<GrainID> grains, SnapperID bid)
    {
        switch (bid.hierarchy)
        {
            case Hierarchy.Global:
                foreach (var id in grains)
                {
                    if (!maxUnCommittedGlobalBidPerGrain.ContainsKey(id) || maxUnCommittedGlobalBidPerGrain[id].Item1.id < bid.id)
                        maxUnCommittedGlobalBidPerGrain[id] = (bid, new TaskCompletionSource());
                }
                break;
            case Hierarchy.Regional:
                foreach (var id in grains)
                {
                    if (!maxUnCommittedRegionalBidPerGrain.ContainsKey(id) || maxUnCommittedRegionalBidPerGrain[id].Item1.id < bid.id)
                        maxUnCommittedRegionalBidPerGrain[id] = (bid, new TaskCompletionSource());
                }
                break;
            case Hierarchy.Local:
                foreach (var id in grains)
                {
                    if (!maxUnCommittedLocalBidPerGrain.ContainsKey(id) || maxUnCommittedLocalBidPerGrain[id].Item1.id < bid.id)
                        maxUnCommittedLocalBidPerGrain[id] = (bid, new TaskCompletionSource());
                }
                break;
        }
    }

    // ================================================================================================ for grain migration

    public async Task FreezeGrain(GrainID grainID)
    {
        // add the grain, so no new transactions (that will access this grain) will be generated 
        if (freezedGrains.ContainsKey(grainID)) throw new SnapperException(ExceptionType.GrainMigration);
        freezedGrains.Add(grainID, new TaskCompletionSource());

        // make sure no more messages will send to this grain

        // STEP 1: check if there are transactions that will be registered
        if (numToBeRegisteredTxnPerGrain.ContainsKey(grainID))
        {
            Debug.Assert(numToBeRegisteredTxnPerGrain[grainID].Get() != 0);
            
            if (allTxnRegisteredPerGrain.ContainsKey(grainID) == false)
                allTxnRegisteredPerGrain[grainID] = new TaskCompletionSource();

            //Console.WriteLine($"PM-{Helper.ConvertGuidToInt(myID.grainID.id)}: wait for {numToBeRegisteredTxnPerGrain[grainID].Get()} transactions related to the grain to register. ");
            await allTxnRegisteredPerGrain[grainID].Task;
        }

        // STEP 2: get the max emitted local and global bid to this grain
        var tasks = new List<Task>();
        if (maxUnCommittedLocalBidPerGrain.ContainsKey(grainID))
        {
            //Console.WriteLine($"PM-{Helper.ConvertGuidToIntString(myID.grainID.id)}: need to wait for local bid = {maxUnCommittedLocalBidPerGrain[grainID].Item1.id} to commit");
            tasks.Add(maxUnCommittedLocalBidPerGrain[grainID].Item2.Task);
        }
        if (maxUnCommittedRegionalBidPerGrain.ContainsKey(grainID))
        {
            //Console.WriteLine($"PM-{Helper.ConvertGuidToIntString(myID.grainID.id)}: need to wait for regional bid = {maxUnCommittedRegionalBidPerGrain[grainID].Item1.id} to commit");
            tasks.Add(maxUnCommittedRegionalBidPerGrain[grainID].Item2.Task);
        }
        if (maxUnCommittedGlobalBidPerGrain.ContainsKey(grainID))
        {
            //Console.WriteLine($"PM-{Helper.ConvertGuidToIntString(myID.grainID.id)}: need to wait for global bid = {maxUnCommittedGlobalBidPerGrain[grainID].Item1.id} to commit");
            tasks.Add(maxUnCommittedGlobalBidPerGrain[grainID].Item2.Task);
        }
        
        // STEP 3: wait until all bacthes related to this grain have committed
        await Task.WhenAll(tasks);
    }

    public Task UnFreezeGrain(GrainID grainID)
    {
        if (!freezedGrains.ContainsKey(grainID)) throw new SnapperException(ExceptionType.GrainMigration);

        freezedGrains[grainID].SetResult();
        freezedGrains.Remove(grainID);

        return Task.CompletedTask;
    }
}
using Concurrency.Common;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using Concurrency.Common.Logging;
using MessagePack;
using Orleans.Concurrency;
using Orleans.Streams;
using Replication.Interface.Coordinator;
using Replication.Interface.GrainReplicaPlacement;
using Replication.Interface.TransactionReplication;
using System.Diagnostics;
using Utilities;

namespace Replication.Implementation.GrainReplicaPlacement;

[Reentrant]
[SnapperReplicaGrainPlacementStrategy]
internal class ReplicaGrainPlacementManager : Grain, IReplicaGrainPlacementManager
{
    Guid guid;
    string myRegionID;
    string mySiloID;
    readonly ISnapperReplicaCache snapperReplicaCache;
    readonly ISnapperSubscriber snapperSubscriber;
    readonly ISnapperClusterCache snapperClusterCache;

    IReplicaRegionalCoordGrain regionalCoordGrain;

    /// <summary> silo ID, the randomly selected local replication coordinator </summary>
    Dictionary<string, IReplicaLocalCoordGrain> localCoordGrains;

    Dictionary<string, IReplicaGrainPlacementManager> pmPerSilo;

    public ReplicaGrainPlacementManager(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, ISnapperSubscriber snapperSubscriber)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.snapperReplicaCache = snapperReplicaCache;
        this.snapperSubscriber = snapperSubscriber;
    }

    public override async Task OnActivateAsync(CancellationToken _)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        guid = Guid.Parse(strs[0]);
        myRegionID = strs[1];
        Debug.Assert(strs[2].Equals(RuntimeIdentity));
        mySiloID = strs[2];

        var myRegionalSiloID = snapperClusterCache.GetRegionalSiloID(myRegionID);
        var regionalCoordID = snapperReplicaCache.GetOneRandomRegionalCoord(myRegionID);
        regionalCoordGrain = GrainFactory.GetGrain<IReplicaRegionalCoordGrain>(regionalCoordID, myRegionID + "+" + myRegionalSiloID);

        var replicaSilosInRegion = snapperReplicaCache.GetLocalSiloList(myRegionID);
        localCoordGrains = new Dictionary<string, IReplicaLocalCoordGrain>();
        replicaSilosInRegion.ForEach(siloID =>
        {
            var localCoordID = snapperReplicaCache.GetOneRandomLocalCoord(myRegionID, siloID);
            var localCoordGrain = GrainFactory.GetGrain<IReplicaLocalCoordGrain>(localCoordID, myRegionID + "+" + siloID);
            localCoordGrains.Add(siloID, localCoordGrain);
        });

        pmPerSilo = new Dictionary<string, IReplicaGrainPlacementManager>();
        var pmList = snapperReplicaCache.GetPMList(myRegionID);
        foreach (var item in pmList)
        {
            var siloID = item.Key;
            var list = item.Value;
            var pmID = list[new Random().Next(0, list.Count)];
            var pm = GrainFactory.GetGrain<IReplicaGrainPlacementManager>(pmID, myRegionID + "+" + siloID);
            pmPerSilo.Add(siloID, pm);
        }

        (var streams, var channels) = snapperSubscriber.GetSubscriptions(RuntimeIdentity, guid);

        var tasks = new List<Task>();
        streams.ForEach(stream => tasks.Add(stream.SubscribeAsync(ProcessLog)));
        await Task.WhenAll(tasks);
    }

    public Task Init() => Task.CompletedTask;

    public Task CheckGC() => Task.CompletedTask;

    async Task ProcessLog(SnapperLog log, StreamSequenceToken _)
    {
        Debug.Assert(log.type == SnapperLogType.Info);

        var content = MessagePackSerializer.Deserialize<InfoLog>(log.content);
        var id = content.id;
        var grains = content.grains;
        try
        {
            (var _, var txnInfo) = ResolveGrainAccessInfo(grains);
            if (txnInfo.Count == 0) return;
            
            var multiSilo = txnInfo.Count > 1;
            var tasks = new List<Task>();

            // need to inform regional silo
            if (multiSilo) tasks.Add(regionalCoordGrain.RegisterAccessInfo(id, txnInfo.Count));

            // then inform each related local silo
            foreach (var item in txnInfo)
            {
                var siloID = item.Key;
                tasks.Add(localCoordGrains[siloID].RegisterAccessInfo(id, multiSilo, item.Value));
            }
            await Task.WhenAll(tasks);
        }
        catch (Exception e) 
        {
            Console.WriteLine($"ProcessLog: id = {id.id}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    /// <summary> resolve the grain access info based on the grain placement in the current region </summary>
    /// <return> the result only include grains that have replicas in the current region </return>>
    (bool, Dictionary<string, HashSet<SnapperGrainID>>) ResolveGrainAccessInfo(HashSet<GrainID> grains)
    {
        var hasNoReplicaInCurrentRegion = false;
        var txnInfo = new Dictionary<string, HashSet<SnapperGrainID>>();  // silo ID, grain ID
        foreach (var grain in grains)
        {
            (var hasReplicaInCurrentRegion, var siloID) = snapperReplicaCache.GetReplicaGrainLocation(grain);
            if (!hasReplicaInCurrentRegion)
            {
                hasNoReplicaInCurrentRegion = true;
                continue;
            }

            if (!txnInfo.ContainsKey(siloID)) txnInfo.Add(siloID, new HashSet<SnapperGrainID>());

            Debug.Assert(!string.IsNullOrEmpty(grain.className));
            var replicaClassName = Helper.GetReplicaClassName(grain.className);
            txnInfo[siloID].Add(new SnapperGrainID(grain.id, myRegionID + "+" + siloID, replicaClassName));
        }
        return (hasNoReplicaInCurrentRegion, txnInfo);
    }

    public async Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc, List<GrainID> grains)
    {
        (var hasNoReplicaInCurrentRegion, var txnInfo) = ResolveGrainAccessInfo(grains.ToHashSet());
        if (hasNoReplicaInCurrentRegion) throw new Exception($"Not all grains have replicas in region {myRegionID}");

        (var hasReplicaInCurrentRegion, var siloID) = snapperReplicaCache.GetReplicaGrainLocation(startGrainID);
        Debug.Assert(hasReplicaInCurrentRegion);
        if (siloID != mySiloID)
        {
            //Console.WriteLine($"PM: forward PACT reqeust to another PM in silo {siloID}");
            var pm = pmPerSilo[siloID];
            return await pm.SubmitTransaction(startGrainID, startFunc, grains);
        }

        var cxt = new TransactionContext(new SnapperID(), new SnapperID());
        try
        {
            
            // get the transaction context
            var time = DateTime.UtcNow;
            var result = new TransactionResult();
            cxt = await NewPACT(txnInfo);
            result.time.Add(BreakDownLatency.REGISTER, (DateTime.UtcNow - time).TotalMilliseconds);
            time = DateTime.UtcNow;
            
            // invoke call on the first grain
            var grain = GrainFactory.GetGrain<ITransactionReplicationGrain>(startGrainID.id, myRegionID + "+" + mySiloID, startGrainID.className);
            var funcResult = await grain.Execute(startFunc, cxt);
            result.MergeResult(funcResult);
            result.resultObj = funcResult.resultObj;
            result.exception = funcResult.exception;
            result.time.Add(BreakDownLatency.GETTURN, (funcResult.timeToGetTurn - time).TotalMilliseconds);
            result.time.Add(BreakDownLatency.GETSTATE, (funcResult.timeToGetState - funcResult.timeToGetTurn).TotalMilliseconds);
            result.time.Add(BreakDownLatency.EXECUTE, (funcResult.timeToFinishExecution - funcResult.timeToGetState).TotalMilliseconds);
            result.time.Add(BreakDownLatency.PREPARE, (DateTime.UtcNow - funcResult.timeToFinishExecution).TotalMilliseconds);
            time = DateTime.UtcNow;
            result.time.Add(BreakDownLatency.COMMIT, (DateTime.UtcNow - time).TotalMilliseconds);
            return result;
        }
        catch (Exception e)
        {
            //Console.WriteLine($"SubmitTransaction (PACT): bid = {cxt.bid.id}, tid = {cxt.tid.id}, grians = {string.Join(", ", grains.Select(x => Helper.ConvertGuidToInt(x.id)))}, {e.Message}");
            //Debug.Assert(false);
            throw;
        }
    }

    public async Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc)
    {
        (var hasReplicaInCurrentRegion, var siloID) = snapperReplicaCache.GetReplicaGrainLocation(startGrainID);
        if (!hasReplicaInCurrentRegion) throw new Exception($"The grain (id = {startGrainID.id}, name = {startGrainID.className}) has no replica in region {myRegionID}");

        var grain = GrainFactory.GetGrain<ITransactionReplicationGrain>(startGrainID.id, myRegionID + "+" + siloID, startGrainID.className);
        try 
        {
            return await grain.ExecuteACT(startFunc);
        }
        catch (Exception e)
        {
            Console.WriteLine($"SubmitTransaction (ACT): {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    async Task<TransactionContext> NewPACT(Dictionary<string, HashSet<SnapperGrainID>> txnInfo)
    {
        var actorAccessInfo = new ActorAccessInfo(new Dictionary<string, Dictionary<string, HashSet<SnapperGrainID>>> { { myRegionID, txnInfo } });

        SnapperID bid;
        SnapperID tid;
        var siloList = actorAccessInfo.GetAccssSilos(myRegionID);
        if (siloList.Count > 1)
        {
            // ================ regional ===========================================================================================================
            (bid, tid) = await regionalCoordGrain.NewPACT(actorAccessInfo);
        }
        else
        {
            // ================ local ===============================================================================================================
            var localSiloID = siloList.First();
            (bid, tid) = await localCoordGrains[localSiloID].NewPACT(actorAccessInfo);
        }
        return new TransactionContext(bid, tid);
    }
}
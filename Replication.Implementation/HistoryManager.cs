using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Common.Logging;
using Concurrency.Common.State;
using Replication.Interface;
using Replication.Interface.Coordinator;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using Utilities;

namespace Replication.Implementation;

using Schedule = Dictionary<SnapperGrainID, Batch>;

public class HistoryManager : IHistoryManager
{
    string myRegionID;
    string mySiloID;
    string myRegionalSiloID;
    Hierarchy myHierarchy;
    IGrainFactory grainFactory;
    readonly ISnapperReplicaCache snapperReplicaCache;

    // ================================================================================================= for replication
    /// <summary> set true when receive the info of a batch / an ACT </summary>
    ConcurrentDictionary<SnapperID, TaskCompletionSource> waitForAccessInfo;

    /// <summary> set true when a replicated batch / ACT can commit </summary>
    ConcurrentDictionary<SnapperID, TaskCompletionSource> waitForCommit;

    /// <summary> 
    /// bool: if it involves multiple silos
    /// in regional silo: the count is the number of local silos involved
    /// in local silo: the count is the numebr of grains involved
    /// </summary>
    ConcurrentDictionary<SnapperID, (bool, CountdownEvent, HashSet<SnapperGrainID>)> accessInfo;

    /// <summary> the replicated grain state, wait to be inserted to grain history </summary>
    ConcurrentDictionary<SnapperID, ConcurrentDictionary<SnapperGrainID, (PrepareLog, SnapperGrainStateUpdates)>> grainStates;

    /// <summary> 
    /// each grain history contains an ordered list of history states
    /// the lock is used for adding or removing nodes from history list
    /// </summary>
    ConcurrentDictionary<SnapperGrainID, (SemaphoreSlim, HistoryList)> grainHistory;

    // ============================================================================ for read-only transactions on replica

    ScheduleGenerator scheduleGenerator;

    /// <summary> used for tid generation </summary>
    SemaphoreSlim topLock;

    /// <summary> key is primary bid, wait for the arrival of batch info sent from other servers </summary>
    ConcurrentDictionary<SnapperID, TaskCompletionSource> waitForBatchInfo;

    /// <summary> key is primary bid, the number of grains accessed by the batch </summary>
    ConcurrentDictionary<SnapperID, CountdownEvent> numGrainPerBatch;

    public HistoryManager(ISnapperReplicaCache snapperReplicaCache)
    {
        this.snapperReplicaCache = snapperReplicaCache;
        waitForAccessInfo = new ConcurrentDictionary<SnapperID, TaskCompletionSource>();
        waitForCommit = new ConcurrentDictionary<SnapperID, TaskCompletionSource>();
        accessInfo = new ConcurrentDictionary<SnapperID, (bool, CountdownEvent, HashSet<SnapperGrainID>)>();
        grainStates = new ConcurrentDictionary<SnapperID, ConcurrentDictionary<SnapperGrainID, (PrepareLog, SnapperGrainStateUpdates)>>();
        grainHistory = new ConcurrentDictionary<SnapperGrainID, (SemaphoreSlim, HistoryList)>();

        topLock = new SemaphoreSlim(1);
        waitForBatchInfo = new ConcurrentDictionary<SnapperID, TaskCompletionSource>();
        numGrainPerBatch = new ConcurrentDictionary<SnapperID, CountdownEvent>();
    }

    public bool CheckGC()
    {
        foreach (var item in grainHistory) item.Value.Item2.CheckGC();

        if (waitForAccessInfo.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: waitForAccessInfo.Count = {waitForAccessInfo.Count}");
        if (waitForCommit.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: waitForCommit.Count = {waitForCommit.Count}");
        if (accessInfo.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: accessInfo.Count = {accessInfo.Count}");
        if (grainStates.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: grainStates.Count = {grainStates.Count}");
        if (waitForBatchInfo.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: waitForBatchInfo.Count = {waitForBatchInfo.Count}");
        if (numGrainPerBatch.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: numGrainPerBatch.Count = {numGrainPerBatch.Count}");

        if (waitForAccessInfo.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: waitForAccessInfo contains id = {waitForAccessInfo.First().Key.id}, silo = {waitForAccessInfo.First().Key.siloID}");
        if (waitForCommit.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: waitForCommit contains id = {waitForCommit.First().Key.id}, silo = {waitForCommit.First().Key.siloID}");
        if (accessInfo.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: accessInfo contains id = {accessInfo.First().Key.id}, silo = {accessInfo.First().Key.siloID}");
        if (grainStates.Count != 0) Console.WriteLine($"HistoryManager-{myHierarchy}: grainStates contains id = {grainStates.First().Key.id}");

        return waitForAccessInfo.Count == 0 &&
               waitForCommit.Count == 0 && 
               accessInfo.Count == 0 &&
               grainStates.Count == 0 &&
               waitForBatchInfo.Count == 0 && 
               numGrainPerBatch.Count == 0;
    }

    public void Init(IGrainFactory grainFactory, string myRegionID, string mySiloID, string myRegionalSiloID, Hierarchy myHierarchy)
    { 
        this.grainFactory = grainFactory;
        this.myRegionID = myRegionID;
        this.mySiloID = mySiloID;
        this.myRegionalSiloID = myRegionalSiloID;
        this.myHierarchy = myHierarchy;
        scheduleGenerator = new ScheduleGenerator(mySiloID, myRegionID, myHierarchy, snapperReplicaCache, new CommitInfo());
    }

    // ================================================================================================================== for replication
    public async Task RegisterGrainState(SnapperGrainID grainID, PrepareLog log, SnapperGrainStateUpdates updates)
    {
        try
        {
            // put the received grain state to a collection first
            var collection = grainStates.GetOrAdd(log.id, new ConcurrentDictionary<SnapperGrainID, (PrepareLog, SnapperGrainStateUpdates)>());
            var added = collection.TryAdd(grainID, (log, updates));
            Debug.Assert(added);

            // wait for the corresponding access info
            var promise = waitForAccessInfo.GetOrAdd(log.id, new TaskCompletionSource());
            Debug.Assert(promise != null);
            await promise.Task;

            bool found;
            Debug.Assert(accessInfo.ContainsKey(log.id));
            (var multiSilo, var count, var grains) = accessInfo[log.id];
            //Console.WriteLine($"Grain {Helper.ConvertGuidToIntString(grainID.grainID.id)}: register state for id = {log.id.id}, count = {count.CurrentCount}");
            if (count.Signal())
            {
                // when all related grain states have been collected
                Debug.Assert(collection.Count == grains.Count);
                collection.Keys.ToList().ForEach(x => Debug.Assert(grains.Contains(x)));

                // get the corresponding grain info list (sorted)
                var infoList = grains.ToImmutableSortedSet().ToList().Select(x =>
                {
                    var grainInfo = grainHistory.GetOrAdd(x, (new SemaphoreSlim(1), new HistoryList(x)));

                    (PrepareLog, SnapperGrainStateUpdates) logInfo;
                    found = collection.TryGetValue(x, out logInfo);
                    Debug.Assert(found);

                    return (grainInfo, logInfo);
                }).ToList();

                // STEP 1: wait for all prevs to finish registration
                //Console.WriteLine($"Ready to register id = {log.id.id}, wait for registration of all prev");
                var tasks = new List<Task>();
                foreach (var (grainInfo, logInfo) in infoList) tasks.Add(grainInfo.Item2.WaitForPrevToRegister(logInfo.Item1.prevId, logInfo.Item1.prevTimestamp));
                await Task.WhenAll(tasks);
                tasks.Clear();

                // STEP 2: get all locks in order
                //Console.WriteLine($"Ready to register id = {log.id.id}, wait for all locks");
                foreach (var (grainInfo, logInfo) in infoList) await grainInfo.Item1.WaitAsync();

                // STEP 3: register and release locks
                //Console.WriteLine($"Ready to register id = {log.id.id}, append grain states and release locks");
                foreach (var (grainInfo, logInfo) in infoList)
                {
                    // append the current replica grain state to grain history
                    grainInfo.Item2.RegisterGrainState(logInfo.Item1, logInfo.Item2);

                    // release lock
                    grainInfo.Item1.Release();
                }

                // inform regional silo
                if (multiSilo)
                {
                    var regionalCoordID = snapperReplicaCache.GetOneRandomRegionalCoord(myRegionID);
                    var regionalCoordGrain = grainFactory.GetGrain<IReplicaRegionalCoordGrain>(regionalCoordID, myRegionID + "+" + myRegionalSiloID);

                    // wait until it is committed in the regional silo
                    await regionalCoordGrain.ACKCompletion(log.id);
                }

                // STEP 4: wait for all prevs to commit
                //Console.WriteLine($"Ready to commit id = {log.id.id}, wait for commit of all prevs");
                foreach (var (grainInfo, _) in infoList) tasks.Add(grainInfo.Item2.WaitForPrevToCommit(log.id));
                await Task.WhenAll(tasks);
                tasks.Clear();

                // STEP 5: commit the nodes on all grains and release locks
                //Console.WriteLine($"Commit id = {log.id.id} on {infoList.Count} grains");
                foreach (var (grainInfo, _) in infoList)
                {
                    await grainInfo.Item1.WaitAsync();
                    grainInfo.Item2.Commit(log.id);
                    grainInfo.Item1.Release();
                }

                // now can commit
                TaskCompletionSource? commit;
                found = waitForCommit.TryGetValue(log.id, out commit);
                Debug.Assert(found);
                Debug.Assert(commit != null);
                commit.SetResult();

                // do garbage collection
                var removed = accessInfo.TryRemove(log.id, out _);
                Debug.Assert(removed);

                removed = waitForCommit.TryRemove(log.id, out _);
                Debug.Assert(removed);
                removed = waitForAccessInfo.TryRemove(log.id, out _);
                Debug.Assert(removed);
                removed = grainStates.TryRemove(log.id, out _);
                Debug.Assert(removed);

                //Console.WriteLine($"finish commit id = {log.id.id} on {infoList.Count} grains");
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"{e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }

    /// <summary> register how many local replica silos are involved </summary>
    public void RegisterAccessInfo(SnapperID id, int numSilos)
    {
        Debug.Assert(myHierarchy == Hierarchy.Regional);

        var added = accessInfo.TryAdd(id, (true, new CountdownEvent(numSilos), new HashSet<SnapperGrainID>()));
        Debug.Assert(added);

        added = waitForCommit.TryAdd(id, new TaskCompletionSource());
        Debug.Assert(added);

        var promise = waitForAccessInfo.GetOrAdd(id, new TaskCompletionSource());
        Debug.Assert(promise != null);
        promise.SetResult();
    }

    /// <summary> register how many replica grains in this local are involved </summary>
    public void RegisterAccessInfo(SnapperID id, bool multiSilo, HashSet<SnapperGrainID> grains)
    {
        Debug.Assert(grains.Count != 0);
        Debug.Assert(myHierarchy == Hierarchy.Local);

        var added = accessInfo.TryAdd(id, (multiSilo, new CountdownEvent(grains.Count), grains));
        Debug.Assert(added);

        added = waitForCommit.TryAdd(id, new TaskCompletionSource());
        Debug.Assert(added);

        var promise = waitForAccessInfo.GetOrAdd(id, new TaskCompletionSource());
        Debug.Assert(promise != null);
        promise.SetResult();
    }
 
    /// <summary> only called by regional coordinator to inform the completion of a node in a silo </summary>
    public async Task ACKCompletion(SnapperID id)
    {
        try
        {
            // wait for access info first
            var promise = waitForAccessInfo.GetOrAdd(id, new TaskCompletionSource());
            Debug.Assert(promise != null);
            await promise.Task;

            TaskCompletionSource? commit;
            var found = waitForCommit.TryGetValue(id, out commit);
            Debug.Assert(found);
            Debug.Assert(commit != null);

            Debug.Assert(accessInfo.ContainsKey(id));
            if (accessInfo[id].Item2.Signal())
            {
                // now can commit the replicated batch / ACT
                commit.SetResult();

                // do garbage collection
                var removed = accessInfo.TryRemove(id, out _);
                Debug.Assert(removed);
                removed = waitForCommit.TryRemove(id, out _);
                Debug.Assert(removed);
                removed = waitForAccessInfo.TryRemove(id, out _);
                Debug.Assert(removed);
            }
            else await commit.Task;
        }
        catch (Exception e)
        {
            Console.WriteLine($"{e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }

    // ================================================================================================= for read-only transactions on replica
    /// <summary> get a tid for an ACT </summary>
    public async Task<SnapperID> NewACT()
    {
        await topLock.WaitAsync();
        var tid = scheduleGenerator.GenerateNewTid();
        topLock.Release();
        return tid;
    }

    /// <summary> use the PACTs received so far to form a batch </summary>
    public async Task<Schedule> GenerateBatch(List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList)
    {
        if (pactList.Count == 0) return new Schedule();

        await topLock.WaitAsync();
        var schedule = scheduleGenerator.GenerateBatch(pactList);
        topLock.Release();

        // need to register the batch on grain schedule
        if (myHierarchy == Hierarchy.Local) await RegisterBatchToGrainHistory(schedule);
        
        return schedule;
    }

    /// <summary> convert received higher level batches in specified order </summary>
    public async Task<List<Schedule>> ProcessBatches(
        SortedDictionary<SnapperID, Batch> batches,
        Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>> higherPACTList)
    {
        if (batches.Count == 0) return new List<Schedule>();
        await topLock.WaitAsync();
        var schedules = scheduleGenerator.ProcessBatches(batches, higherPACTList);
        topLock.Release();

        if (myHierarchy == Hierarchy.Local)
        {
            foreach (var schedule in schedules)
                await RegisterBatchToGrainHistory(schedule);
        }
        return schedules;
    }

    /// <summary> register a local batch atomically for multiple grains </summary>
    async Task RegisterBatchToGrainHistory(Schedule schedule)
    {
        try
        {
            var primaryBid = schedule.First().Value.GetPrimaryBid();

            // get the corresponding grain info list (sorted)
            var grainInfoList = schedule.Keys.ToImmutableSortedSet().ToList().Select(grainID => grainHistory.GetOrAdd(grainID, (new SemaphoreSlim(1), new HistoryList(grainID)))).ToList();
            //Console.WriteLine($"try register batch id = {primaryBid.id} on {grainInfoList.Count} grains");
            // get all lcoks
            foreach (var x in grainInfoList) await x.Item1.WaitAsync();

            foreach (var x in grainInfoList)
            {
                // register batch info
                var transactions = schedule[x.Item2.myGrainID].txnList.Select(x => x.Item1).ToList();
                x.Item2.RegisterBatch(primaryBid, transactions);

                // release locks
                x.Item1.Release();
                //Console.WriteLine($"Grain {Helper.ConvertGuidToInt(x.Item2.myGrainID.id)}: finish register batch id = {primaryBid.id}");
            }
            //Console.WriteLine($"finish register batch id = {primaryBid.id} on {grainInfoList.Count} grains");
            var added = numGrainPerBatch.TryAdd(primaryBid, new CountdownEvent(schedule.Count));
            Debug.Assert(added);

            // mark that the new batch has been added to the grain schedule
            var promise = waitForBatchInfo.GetOrAdd(primaryBid, new TaskCompletionSource());
            promise.SetResult();
        }
        catch (Exception e)
        {
            Console.WriteLine($"RegisterBatchToGrainHistory: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task<SnapperGrainState> WaitForTurn(SnapperGrainID grainID, TransactionContext cxt)
    {
        try
        {
            Debug.Assert(grainHistory.ContainsKey(grainID));
            var grainInfo = grainHistory[grainID];

            if (!cxt.isDet)
            {
                await grainInfo.Item1.WaitAsync();
                grainInfo.Item2.RegisterACT(cxt.tid);
                grainInfo.Item1.Release();
            }
            else
            {
                // if the batch info hasn't arrived yet, need to wait
                var promise = waitForBatchInfo.GetOrAdd(cxt.bid, new TaskCompletionSource());
                await promise.Task;
            }

            var res = await grainInfo.Item2.WaitForTurn(cxt);
            return res;
        }
        catch (SnapperException)
        {
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine($"WaitForTurn: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task CompleteTxn(SnapperGrainID grainID, TransactionContext cxt)
    {
        try
        {
            (SemaphoreSlim, HistoryList) grainInfo;
            if (!grainHistory.ContainsKey(grainID))
            {
                Debug.Assert(grainID.grainID.className.Contains("SellerActor"));
                grainInfo = grainHistory.GetOrAdd(grainID, (new SemaphoreSlim(1), new HistoryList(grainID)));
            }
            else grainInfo = grainHistory[grainID];

            await grainInfo.Item1.WaitAsync();
            var batchComplete = grainInfo.Item2.CompleteTxn(cxt);
            grainInfo.Item1.Release();

            if (batchComplete && cxt.isDet)
            {
                CountdownEvent? count;
                var found = numGrainPerBatch.TryGetValue(cxt.bid, out count);
                Debug.Assert(found);
                Debug.Assert(count != null);

                if (count.Signal())
                {
                    var removed = waitForBatchInfo.TryRemove(cxt.bid, out _);
                    Debug.Assert(removed);

                    removed = numGrainPerBatch.TryRemove(cxt.bid, out _);
                    Debug.Assert(removed);
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"CompleteTxn: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }
}
using Utilities;
using Orleans.Concurrency;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Implementation.GrainPlacement;
using Orleans.GrainDirectory;
using Concurrency.Common;
using System.Diagnostics;
using Concurrency.Interface;
using Concurrency.Interface.Coordinator;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using Concurrency.Common.Logging;
using Replication.Interface.TransactionReplication;
using Concurrency.Common.State;
using Concurrency.Common.Scheduling;
using Concurrency.Interface.GrainPlacement;
using Orleans.Runtime;
using System.Security.Cryptography;

namespace Concurrency.Implementation.TransactionExecution;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public abstract class TransactionExecutionGrain : Grain, ITransactionExecutionGrain
{
    // grain basic info
    readonly string myNameSpace;
    public SnapperGrainID myID;
    string myRegionID;
    string mySiloID;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly ISnapperReplicaCache snapperReplicaCache;
    readonly ISnapperLoggingHelper log;
    readonly bool speculativeACT;
    readonly bool speculativeBatch;
    CommitInfo commitInfo;
    ExperimentData experimentData;

    // transaction execution
    IGrainPlacementManager myPM;
    IGlobalCoordGrain myGlobalCoordGrain;
    ScheduleList scheduleList;
    GrainStateManager stateManager;
    TransactionalAPI transactionalAPI;
    Dictionary<SnapperID, FunctionResult> funcResults;    // tid, result
    
    // replication
    DateTime prevTimestamp;
    SnapperID prevPreparedVersion;

    // key reference management
    KeyReferenceManager keyReferenceManager;

    // for debugging only !!!
    bool hasReplica = false;

    // for grain migration
    bool underMigration;
    HashSet<SnapperID> unfinishedACTOnGrain;    // keep track of the total number of active ACTs on this grain
    TaskCompletionSource allACTsComplete;          // set completed when all ACTs have completed

    public TransactionExecutionGrain(string? myNameSpace, ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
    {
        Debug.Assert(!string.IsNullOrEmpty(myNameSpace));
        this.myNameSpace = myNameSpace;
        this.snapperClusterCache = snapperClusterCache;
        this.snapperReplicaCache = snapperReplicaCache;
        this.log = log;
        (speculativeACT, speculativeBatch) = snapperClusterCache.IsSpeculative();
    }

    public Task<AggregatedExperimentData> CheckGC()
    {
        stateManager.CheckGC();
        scheduleList.CheckGC();
        if (funcResults.Count != 0) Console.WriteLine($"TransactionExecutionGrain: funcResults.Count = {funcResults.Count}");

        var result = experimentData.AggregateAndClear();
        return Task.FromResult(result);
    }

    /// <summary> Any type of message may cause the grain being activated </summary>
    public override Task OnActivateAsync(CancellationToken _)
    {
        // for grain migration
        underMigration = true;
        unfinishedACTOnGrain = new HashSet<SnapperID>();
        allACTsComplete = new TaskCompletionSource();
        
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], myNameSpace);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);
        (var speculativeACT, var speculativeBatch) = snapperClusterCache.IsSpeculative();
        commitInfo = new CommitInfo();
        experimentData = new ExperimentData();
        funcResults = new Dictionary<SnapperID, FunctionResult>();
        stateManager = new GrainStateManager(myID);
        scheduleList = new ScheduleList(myID, speculativeACT, speculativeBatch, commitInfo, experimentData);
        transactionalAPI = new TransactionalAPI(myID, myRegionID, GrainFactory, snapperClusterCache, scheduleList, stateManager, funcResults);
        
        prevTimestamp = DateTime.UtcNow;
        prevPreparedVersion = new SnapperID();
        keyReferenceManager = new KeyReferenceManager(myID.grainID, transactionalAPI);

        var pmGuid = snapperClusterCache.GetOneRandomPlacementManager(myRegionID, mySiloID);
        myPM = GrainFactory.GetGrain<IGrainPlacementManager>(pmGuid, myRegionID + "+" + mySiloID);

        (var globalRegion, var globalSilo) = snapperClusterCache.GetGlobalRegionAndSilo();
        var globalCoordID = snapperClusterCache.GetOneRandomGlobalCoord();
        if (globalRegion != myRegionID)
        {
            var client = snapperClusterCache.GetOrleansClient(globalRegion);
            myGlobalCoordGrain = client.GetGrain<IGlobalCoordGrain>(globalCoordID, globalRegion + "+" + globalSilo);
        }
        else myGlobalCoordGrain = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID, globalRegion + "+" + globalSilo);

        if (myID.grainID.className.Contains("AccountGrain") ||
            myID.grainID.className.Contains("TestGrain") ||
            myID.grainID.className.Contains("SellerActor") ||
            myID.grainID.className.Contains("StockActor") ||
            myID.grainID.className.Contains("ShipmentActor"))
            hasReplica = true;

        return Task.CompletedTask;
    }

    Task ITransactionExecutionGrain.ReceiveBatch(Immutable<Batch> batch, CommitInfo newCommitInfo)
    {
        commitInfo.MergeCommitInfo(newCommitInfo);
        scheduleList.RegisterBatch(batch.Value);
        return Task.CompletedTask;
    }

    Task ITransactionExecutionGrain.CommitBatch(SnapperID primaryBid, CommitInfo newCommitInfo)
    {
        commitInfo.MergeCommitInfo(newCommitInfo);
        scheduleList.CommitBatch(primaryBid);
        return Task.CompletedTask;
    }

    async Task<TransactionResult> ITransactionExecutionGrain.ExecutePACT(FunctionCall startFunc, TransactionContext cxt, DateTime registeredTime)
    {
        var result = new TransactionResult();
        var exeFinished = false;

        try
        {
            var funcResult = await Execute(startFunc, cxt);
            exeFinished = true;
            result.MergeResult(funcResult);
            result.time.Add(BreakDownLatency.GETSCHEDULE, (funcResult.timeToGetScheduleReady - registeredTime).TotalMilliseconds);
            result.time.Add(BreakDownLatency.GETTURN, (funcResult.timeToGetTurn - funcResult.timeToGetScheduleReady).TotalMilliseconds);
            result.time.Add(BreakDownLatency.GETSTATE, (funcResult.timeToGetState - funcResult.timeToGetTurn).TotalMilliseconds);
            result.time.Add(BreakDownLatency.EXECUTE, (funcResult.timeToFinishExecution - funcResult.timeToGetState).TotalMilliseconds);
            result.time.Add(BreakDownLatency.PREPARE, (DateTime.UtcNow - funcResult.timeToFinishExecution).TotalMilliseconds);
            var time = DateTime.UtcNow;

            if (!commitInfo.isBatchCommit(cxt.bid)) await scheduleList.WaitForBatchCommit(cxt.bid);
            //Console.WriteLine($"grain commit bid = {cxt.bid.Print()} time = {(DateTime.UtcNow - DateTime.MinValue).TotalMilliseconds}");
            result.time.Add(BreakDownLatency.COMMIT, (DateTime.UtcNow - time).TotalMilliseconds);

            result.resultObj = funcResult.resultObj;
            result.exception = funcResult.exception;

            return result;
        }
        catch (Exception e)
        {
            Console.WriteLine($"SubmitTransaction (PACT): bid = {cxt.bid.Print()}, exe finish = {exeFinished}, {e.Message}, {e.StackTrace}");
            throw;
        }
    }

    async Task<TransactionResult> ITransactionExecutionGrain.ExecuteACT(FunctionCall startFunc)
    {
        var result = new TransactionResult();
        if (underMigration)
        {
            result.SetException(ExceptionType.GrainMigration);
            return result;
        }

        // get the transaction context
        var time = DateTime.UtcNow;
        (var tid, var newCommitInfo) = await myGlobalCoordGrain.NewACT();
        commitInfo.MergeCommitInfo(newCommitInfo);

        //Console.WriteLine($"grain {myID.grainID.Print()}: receive ACT tid = {tid.Print()}");
        var cxt = new TransactionContext(tid);
        result.time.Add(BreakDownLatency.REGISTER, (DateTime.UtcNow - time).TotalMilliseconds);
        time = DateTime.UtcNow;

        try
        {
            // invoke call on the first grain
            var funcResult = await Execute(startFunc, cxt);
            result.time.Add(BreakDownLatency.GETSCHEDULE, (funcResult.timeToGetScheduleReady - time).TotalMilliseconds);
            result.time.Add(BreakDownLatency.GETTURN, (funcResult.timeToGetTurn - funcResult.timeToGetScheduleReady).TotalMilliseconds);
            result.time.Add(BreakDownLatency.GETSTATE, (funcResult.timeToGetState - funcResult.timeToGetTurn).TotalMilliseconds);
            result.time.Add(BreakDownLatency.EXECUTE, (funcResult.timeToFinishExecution - funcResult.timeToGetState).TotalMilliseconds);
            result.MergeResult(funcResult);

            // do 2pc
            ITransactionExecutionGrain grain;
            var tasks = new List<Task>();
            if (!result.hasException())
            {
                // prepare (sent to grains that have granted this ACT read or write locks)
                var prepareTasks = new Dictionary<SnapperGrainID, Task<(bool, bool)>>();
                foreach (var grainID in result.grains)
                {
                    (var regionID, _) = grainID.GetLocation();

                    if (regionID != myRegionID)
                    {
                        var client = snapperClusterCache.GetOrleansClient(regionID);
                        grain = client.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);
                    }
                    else grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);

                    prepareTasks.Add(grainID, grain.Prepare(cxt.tid));
                }
                await Task.WhenAll(prepareTasks.Values);

                // find the grains that actually have information to persist or replicate
                var needToPersist = prepareTasks.Where(x => x.Value.Result.Item1).Select(x => x.Key).ToHashSet();
                Debug.Assert(needToPersist != null);
                var stateChanged = prepareTasks.Where(x => x.Value.Result.Item2).Select(x => x.Key).ToHashSet();
                Debug.Assert(stateChanged != null);

                if (needToPersist.Count != 0)
                {
                    var writerSet = needToPersist.Select(x => x.grainID).ToHashSet();
                    if (log.DoLogging()) await log.InfoLog(cxt.tid, writerSet);
                    if (log.DoReplication()) await log.InfoEvent(cxt.tid, writerSet);
                }

                result.time.Add(BreakDownLatency.PREPARE, (DateTime.UtcNow - funcResult.timeToFinishExecution).TotalMilliseconds);
                time = DateTime.UtcNow;
                prepareTasks.Clear();

                // commit (only sent to grains that this ACT has made changes to)
                tasks.Clear();
                foreach (var grainID in stateChanged)
                {
                    (var regionID, _) = grainID.GetLocation();

                    if (regionID != myRegionID)
                    {
                        var client = snapperClusterCache.GetOrleansClient(regionID);
                        grain = client.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);
                    }
                    else grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);

                    tasks.Add(grain.Commit(cxt.tid));
                }

                result.resultObj = funcResult.resultObj;

                await Task.WhenAll(tasks);
                tasks.Clear();
            }
            else
            {
                // abort (sent to grains that have granted this ACT read or write locks)
                foreach (var grainID in result.grains)
                {
                    (var regionID, _) = grainID.GetLocation();

                    if (regionID != myRegionID)
                    {
                        var client = snapperClusterCache.GetOrleansClient(regionID);
                        grain = client.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);
                    }
                    else grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);

                    tasks.Add(grain.Abort(cxt.tid));
                }
                await Task.WhenAll(tasks);
                tasks.Clear();
            }
            result.time.Add(BreakDownLatency.COMMIT, (DateTime.UtcNow - time).TotalMilliseconds);
            //Console.WriteLine($"grain {myID.grainID.Print()}: return ACT tid = {tid.Print()}");
            return result;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Grain {myID.grainID.Print()}: ExecuteACT {startFunc.funcName}, tid = {cxt.tid.id}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task<FunctionResult> Execute(FunctionCall call, TransactionContext cxt)
    {
        var time = DateTime.UtcNow;
        if (!cxt.isDet)
        {
            if (underMigration)
            {
                var res = new FunctionResult();
                res.SetException(ExceptionType.GrainMigration);
                return res;
            }
            else unfinishedACTOnGrain.Add(cxt.tid);
        }
        
        // invoke the function call on this grain
        var funcResult = new FunctionResult();
        try
        {
            // invoke the user specified function
            var resultObj = await InvokeFunction(call, cxt);
            funcResult.timeToFinishExecution = DateTime.UtcNow;
            experimentData.Set(MonitorTime.Execute, (DateTime.UtcNow - time).TotalMilliseconds);

            // set the result for this function call
            if (resultObj != null) funcResult.SetResult(resultObj);
        }
        catch (SnapperException e)
        {
            Debug.Assert(!cxt.isDet);
            funcResult.SetException(e.exceptionType);
        }
        catch (Exception e)
        {
            // this should never happen
            Console.WriteLine($"Grain {myID.grainID.Print()}: tid = {cxt.tid.id}, bid = {cxt.bid.id}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }

        time = DateTime.UtcNow;
        if (funcResults.ContainsKey(cxt.tid))
        {
            funcResult.MergeResult(funcResults[cxt.tid]);
            funcResult.timeToGetScheduleReady = funcResults[cxt.tid].timeToGetScheduleReady;
            funcResult.timeToGetTurn = funcResults[cxt.tid].timeToGetTurn;
            funcResult.timeToGetState = funcResults[cxt.tid].timeToGetState;
        }

        if (cxt.isDet)
        {
            try
            {
                Debug.Assert(!funcResult.hasException());

                stateManager.SetState(cxt);
                (var batchCompleted, var coordID) = scheduleList.CompleteTxn(cxt);
                if (batchCompleted)
                {
                    Debug.Assert(coordID != null);

                    var implementationType = stateManager.GetType(cxt.tid);
                    var isWriter = await PrepareState(true, cxt.bid, cxt.tid, implementationType);
                    var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(coordID.grainID.id, coordID.location);
                    _ = localCoord.ACKBatchCompleteOnGrain(myID, cxt.bid, isWriter);

                    stateManager.DeleteState(cxt);
                }

                stateManager.DeleteState(cxt.tid);
            }
            catch (Exception e)
            {
                Console.WriteLine($"{myID.grainID.Print()}: {e.Message}, {e.StackTrace}");
                Debug.Assert(false);
            }
        }
        else
        {
            if (stateManager.IfLockGranted(cxt.tid))
            {
                if (funcResult.hasException())
                {
                    funcResult.grains.Remove(myID);
                    CompleteAnACT(cxt.tid);
                }
                else Debug.Assert(funcResult.grains.Contains(myID));
            }
            else
            {
                funcResult.grains.Remove(myID);
                CompleteAnACT(cxt.tid);
            }
        }

        funcResults.Remove(cxt.tid);
        experimentData.Set(MonitorTime.Prepare, (DateTime.UtcNow - time).TotalMilliseconds);
        return funcResult;
    }

    public async Task<object?> CallGrain(GrainID grainID, FunctionCall call, TransactionContext cxt) => await transactionalAPI.CallGrain(grainID, call, cxt);
    
    public async Task<object?> InvokeFunction(FunctionCall call, TransactionContext cxt)
    {
        // check if it's internal functions
        if (call.type == null)
        {
            var func = Enum.Parse<SnapperInternalFunction>(call.funcName);
            switch (func)
            {
                case SnapperInternalFunction.NoOp:
                    if (cxt.isDet)
                    {
                        await scheduleList.WaitForScheduleInfo(cxt);
                        var timeToGetScheduleReady = DateTime.UtcNow;
                        var timeToGetTurn = DateTime.UtcNow;

                        if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
                        funcResults[cxt.tid].AddGrain(myID);
                        funcResults[cxt.tid].timeToGetScheduleReady = timeToGetScheduleReady;
                        funcResults[cxt.tid].timeToGetTurn = timeToGetTurn;
                        funcResults[cxt.tid].timeToGetState = DateTime.UtcNow;
                    }
                    return null;

                // =================================================================================================
                // only happen on the origin grains (data model, actor-level concurrency control)
                case SnapperInternalFunction.RegisterReference: return await keyReferenceManager.RegisterReference(ImplementationType.SNAPPER, cxt, call.funcInput);
                case SnapperInternalFunction.DeRegisterReference: return await keyReferenceManager.DeRegisterReference(ImplementationType.SNAPPER, cxt, call.funcInput);
                // only happen on the origin grains (data model, key-level concurrency control)
                case SnapperInternalFunction.RegisterFineReference: return await keyReferenceManager.RegisterReference(ImplementationType.SNAPPERFINE, cxt, call.funcInput);
                case SnapperInternalFunction.DeRegisterFineReference: return await keyReferenceManager.DeRegisterReference(ImplementationType.SNAPPERFINE, cxt, call.funcInput);

                // =================================================================================================
                // only happen on follower grains (data model, actor-level concurrency control)
                case SnapperInternalFunction.ApplyForwardedKeyOperations: return await keyReferenceManager.ApplyForwardedKeyOperations(ImplementationType.SNAPPER, cxt, call.funcInput);
                // only happen on follower grains (data model, key-level concurrency control)
                case SnapperInternalFunction.ApplyForwardedFineKeyOperations: return await keyReferenceManager.ApplyForwardedKeyOperations(ImplementationType.SNAPPERFINE, cxt, call.funcInput);

                // =================================================================================================
                case SnapperInternalFunction.ReadState: return await TakeSnapshotOfGrainState(cxt);

                default: throw new Exception($"The internal function {call.funcName} is not supported. ");
            }
        }
        else
        {
            // resolve the function call
            var method = call.type.GetMethod(call.funcName);
            if (method == null) throw new Exception($"Fail to find the method {call.funcName} on type {call.type.AssemblyQualifiedName}");

            // invoke the function call
            try
            {
                var task = method.Invoke(this, new object[] { cxt, call.funcInput }) as Task<object?>;
                if (task == null) throw new Exception($"Fail to invoke the method {call.funcName} on type {call.type.AssemblyQualifiedName}");

                // execute the function call
                var res = await task;

                // check if need to forward updates to followers (only if the current transaction has done writes)
                var updatesPerGrain = stateManager.GetUpdatesToForward(cxt.tid);
                if (updatesPerGrain.Count != 0) await keyReferenceManager.ForwardKeyOperations(stateManager.GetType(cxt.tid), cxt, updatesPerGrain);
               
                return res;
            }
            catch (SnapperException)
            {
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Fail to invke {call.funcName} on {myID.grainID.className}, {e.Message}, {e.StackTrace}");
                Debug.Assert(false);
                throw;
            }
        }
    }

    public async Task<(IDictionaryState, IListState)> GetSimpleState(AccessMode mode, TransactionContext cxt) => await transactionalAPI.GetInternalSimpleState(mode, cxt);

    public async Task<(IDictionaryState, IListState)> GetKeyValueState(AccessMode mode, TransactionContext cxt, HashSet<ISnapperKey> keys) => await transactionalAPI.GetInternalKeyValueState(mode, cxt, keys);

    public async Task<(IDictionaryState, IListState)> GetFineState(TransactionContext cxt, Dictionary<ISnapperKey, AccessMode> keys) => await transactionalAPI.GetInternalFineState(cxt, keys);

    /// <summary> this message is sent from coordinator to parcipant actors when read or write locks have been acquired </summary>
    /// <return> if there is anything to persist, if the grain state has been changed by the ACT </return>
    async Task<(bool, bool)> ITransactionExecutionGrain.Prepare(SnapperID tid)
    {
        Debug.Assert(stateManager.IfLockGranted(tid));
        var needToPersist = await PrepareState(false, new SnapperID(), tid, stateManager.GetType(tid));
        var stateChanged = stateManager.IfStateChanged(tid);
        if (needToPersist) Debug.Assert(stateChanged);
        if (!stateChanged) Debug.Assert(!needToPersist);

        if (!stateChanged) CompleteAnACT(tid);
        
        return (needToPersist, stateChanged);
    }

    /// <summary> the commit messaeg is sent after all reader grains have released locks and writer grains have persisted states </summary>
    async Task ITransactionExecutionGrain.Commit(SnapperID tid)
    {
        Debug.Assert(stateManager.IfStateChanged(tid) && stateManager.IfLockGranted(tid));

        if (log.DoLogging()) await log.CommitLog(tid);
        stateManager.SetState(tid);

        CompleteAnACT(tid);
    }

    /// <summary> the abort messaeg is sent if exception happened to the ACT </summary>
    Task ITransactionExecutionGrain.Abort(SnapperID tid)
    {
        CompleteAnACT(tid);
        return Task.CompletedTask;
    }

    /// <returns> if the batch / ACT has anything to persist </returns>
    async Task<bool> PrepareState(bool isDet, SnapperID bid, SnapperID tid, ImplementationType implementationType)
    {
        var needToPersist = false;
        var id = isDet ? bid : tid;
        var timestamp = DateTime.UtcNow;
        Debug.Assert(prevTimestamp < timestamp);

        if (log.DoLogging() || log.DoReplication())
        {
            var stateUpdates = stateManager.TakeSnapshot(isDet, bid, tid, implementationType);

            if (stateUpdates.Item1 == null)
            {
                Debug.Assert(stateUpdates.Item2 == null && stateUpdates.Item3 == null);
                return needToPersist;
            }
            else Debug.Assert(stateUpdates.Item2 != null && stateUpdates.Item3 != null);

            needToPersist = true;
            
            if (log.DoLogging()) await log.PrepareLog(myID.grainID, id, timestamp, prevPreparedVersion, prevTimestamp, stateUpdates.Item1, stateUpdates.Item2, stateUpdates.Item3);
            if (log.DoReplication())
            {
                //await log.PrepareEvent(myID.grainID, id, timestamp, prevPreparedVersion, prevTimestamp, stateUpdates.Item1, stateUpdates.Item2, stateUpdates.Item3);

                // this is for debugging only!!!
                if (hasReplica)
                {
                    var logRecord = new PrepareLog(myID.grainID, id, timestamp, prevPreparedVersion, prevTimestamp, stateUpdates.Item1, stateUpdates.Item2, stateUpdates.Item3);

                    var replicaGrainID = new GrainID(myID.grainID.id, Helper.GetReplicaClassName(myID.grainID.className));
                    (_, var replicaSiloAddress) = snapperReplicaCache.GetReplicaGrainLocation(replicaGrainID);
                    if (string.IsNullOrEmpty(replicaSiloAddress)) Console.WriteLine($"{myID.grainID.className} || {Helper.GetReplicaClassName(myID.grainID.className)}");
                    Debug.Assert(!string.IsNullOrEmpty(replicaSiloAddress));

                    try
                    {
                        var replicaGrain = GrainFactory.GetGrain<ITransactionReplicationGrain>(replicaGrainID.id, myRegionID + "+" + replicaSiloAddress, replicaGrainID.className);
                        await replicaGrain.RegisterGrainState(logRecord);
                    }
                    catch (Exception e)
                    {
                        // if fail to find the replica grain, it means no need to replica this grain state
                        if (!e.Message.Contains("Could not find an implementation matching prefix") || !e.Message.Contains("MarketPlace.Grains.SnapperReplicatedGrains")) throw;
                    }
                }
            }
        }
        
        prevTimestamp = timestamp;
        prevPreparedVersion = id;
        return needToPersist;
    }

    // ========================================================================================================================================
    public async Task<bool> RegisterSimpleReference(TransactionContext cxt, ReferenceInfo referenceInfo, ISnapperValue? value2 = null) => await transactionalAPI.RegisterReference(ImplementationType.SNAPPERSIMPLE, cxt, referenceInfo, value2);

    public async Task<bool> RegisterReference(TransactionContext cxt, ReferenceInfo referenceInfo, ISnapperValue? value2 = null) => await transactionalAPI.RegisterReference(ImplementationType.SNAPPER, cxt, referenceInfo, value2);

    public async Task<bool> RegisterFineReference(TransactionContext cxt, ReferenceInfo referenceInfo, ISnapperValue? value2 = null) => await transactionalAPI.RegisterReference(ImplementationType.SNAPPERFINE, cxt, referenceInfo, value2);

    // ========================================================================================================================================
    public async Task<(byte[], byte[], byte[], byte[])> TakeSnapshotOfGrainState(TransactionContext cxt)
    {
        (var dictionaryState, var listState) = await transactionalAPI.GetInternalSimpleState(AccessMode.Read, cxt);

        var dictionary = dictionaryState.GetAllItems();

        var updateReference = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        var deleteReference = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        var references = dictionaryState.GetAllReferences();
        if (references.ContainsKey(SnapperKeyReferenceType.ReplicateReference)) updateReference = references[SnapperKeyReferenceType.ReplicateReference];
        if (references.ContainsKey(SnapperKeyReferenceType.DeleteReference)) deleteReference = references[SnapperKeyReferenceType.DeleteReference];

        var list = listState.GetAllEntries();

        return SerializeGrainState(dictionary, updateReference, deleteReference, list);
    }

    (byte[], byte[], byte[], byte[]) SerializeGrainState(
        Dictionary<ISnapperKey, SnapperValue> dictionary,
        Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>> updateReference,
        Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>> deleteReference,
        List<SnapperRecord> list)
    {
        // serialize dictionary state
        var item1 = SnapperSerializer.Serialize(dictionary);

        // serialize key references info
        var item2 = SnapperSerializer.Serialize(updateReference);
        var item3 = SnapperSerializer.Serialize(deleteReference);

        // serialize list state
        var item4 = SnapperSerializer.Serialize(list);

        return (item1, item2, item3, item4);
    }

    void CompleteAnACT(SnapperID tid)
    {
        try
        {
            stateManager.DeleteStateAndReleaseLock(tid);
            scheduleList.CompleteTxn(tid);

            if (!unfinishedACTOnGrain.Contains(tid)) return;

            Debug.Assert(unfinishedACTOnGrain.Contains(tid));
            unfinishedACTOnGrain.Remove(tid);

            if (unfinishedACTOnGrain.Count == 0)
            {
                allACTsComplete.SetResult();
                allACTsComplete = new TaskCompletionSource();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"grain {myID.grainID.Print()}: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }

    public Task ActivateGrain()
    {
        underMigration = false;
        return Task.CompletedTask;
    }

    public Task ActivateGrain(DateTime prevTimestamp, SnapperID prevPreparedVersion, byte[] dictionary, byte[] updateReference, byte[] deleteReference, byte[] list)
    {
        var dictionaryState = SnapperSerializer.DeserializeDictionary(dictionary);
        var updateReferenceState = SnapperSerializer.DeserializeReferences(updateReference);
        var deleteReferenceState = SnapperSerializer.DeserializeReferences(deleteReference);
        var referenceState = new Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>
        {
            { SnapperKeyReferenceType.ReplicateReference, updateReferenceState },
            { SnapperKeyReferenceType.DeleteReference, deleteReferenceState }
        };
        var listState = SnapperSerializer.DeserializeUpdatesOnList(list);
        stateManager.SetInitialState(dictionaryState, referenceState, listState);

        this.prevTimestamp = prevTimestamp;
        this.prevPreparedVersion = prevPreparedVersion;

        underMigration = false;

        return Task.CompletedTask;
    }

    public async Task FreezeGrain()
    {
        underMigration = true;
        
        if (unfinishedACTOnGrain.Count != 0) await allACTsComplete.Task;
    }

    public Task<(DateTime, SnapperID, byte[], byte[], byte[], byte[])> DeactivateGrain()
    {
        DeactivateOnIdle();

        (var dictionary, var references, var list) = stateManager.GetState();
        var updateReference = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        var deleteReference = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        if (references.ContainsKey(SnapperKeyReferenceType.ReplicateReference)) updateReference = references[SnapperKeyReferenceType.ReplicateReference];
        if (references.ContainsKey(SnapperKeyReferenceType.DeleteReference)) deleteReference = references[SnapperKeyReferenceType.DeleteReference];

        (var item1, var item2, var item3, var item4) = SerializeGrainState(dictionary, updateReference, deleteReference, list);
        return Task.FromResult((prevTimestamp, prevPreparedVersion, item1, item2, item3, item4));
    }
}
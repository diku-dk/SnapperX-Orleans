using Concurrency.Common.Logging;
using Concurrency.Common;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Orleans.Streams;
using Utilities;
using Replication.Interface.TransactionReplication;
using Replication.Implementation.GrainReplicaPlacement;
using MessagePack;
using Orleans.Runtime;
using Replication.Interface;
using System.Diagnostics;
using Replication.Interface.Coordinator;
using Concurrency.Common.ICache;
using Concurrency.Common.State;

namespace Replication.Implementation.TransactionReplication;

[Reentrant]
[SnapperReplicaGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public abstract class TransactionReplicationGrain : Grain, ITransactionReplicationGrain
{
    // grain basic info
    readonly string myNameSpace;
    public SnapperGrainID myID;
    string myRegionID;

    readonly IHistoryManager historyManager;
    readonly ISnapperReplicaCache snapperReplicaCache;
    readonly ISnapperClusterCache snapperClusterCache;

    // for transaction execution
    IReplicaRegionalCoordGrain myReplicaRegionalCoordGrain;
    Dictionary<SnapperID, FunctionResult> funcResults;

    public TransactionReplicationGrain(string? myNameSpace, IHistoryManager historyManager, ISnapperReplicaCache snapperReplicaCache, ISnapperClusterCache snapperClusterCache)
    {
        Debug.Assert(!string.IsNullOrEmpty(myNameSpace));
        this.myNameSpace = myNameSpace;
        this.historyManager = historyManager;
        this.snapperReplicaCache = snapperReplicaCache;
        this.snapperClusterCache = snapperClusterCache;
    }

    public Task Init() => Task.CompletedTask;

    public Task CheckGC()
    {
        if (funcResults.Count != 0) Console.WriteLine($"TransactionReplicationGrain: funcResults.Count = {funcResults.Count}");
        return Task.CompletedTask;
    }

    public override async Task OnActivateAsync(CancellationToken _)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myRegionID = strs[1];
        Debug.Assert(RuntimeIdentity.Equals(strs[2]));
        myID = new SnapperGrainID(Guid.Parse(strs[0]), myRegionID + '+' + RuntimeIdentity, myNameSpace);

        var replicaRegionalCoordID = snapperReplicaCache.GetOneRandomRegionalCoord(myRegionID);
        var regionalSiloID = snapperClusterCache.GetRegionalSiloID(myRegionID);
        myReplicaRegionalCoordGrain = GrainFactory.GetGrain<IReplicaRegionalCoordGrain>(replicaRegionalCoordID, myRegionID + "+" + regionalSiloID);

        funcResults = new Dictionary<SnapperID, FunctionResult>();

        var streamProvider = this.GetStreamProvider(Constants.DefaultStreamProvider);
        var stream = streamProvider.GetStream<SnapperLog>(StreamId.Create(SnapperLogType.Prepare + "-" + myNameSpace.ToString(), myID.grainID.id));
        await stream.SubscribeAsync(ProcessLog);
    }

    public async Task RegisterGrainState(PrepareLog content)
    {
        try
        {
            var updatesOnDictionary = SnapperSerializer.DeserializeUpdatesOnDictionary(content.updatesOnDictionary);
            var updatesOnReference = SnapperSerializer.DeserializeUpdatesOnReference(content.updatesOnReference);
            var updatesOnList = SnapperSerializer.DeserializeUpdatesOnList(content.updatesOnList);
            _ = historyManager.RegisterGrainState(myID, content, new SnapperGrainStateUpdates(updatesOnDictionary, updatesOnReference, updatesOnList));

            await Task.CompletedTask;
        }
        catch (Exception e)
        {
            Console.WriteLine($"{e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }

    async Task ProcessLog(SnapperLog log, StreamSequenceToken _)
    {
        try
        {
            Debug.Assert(log.type == SnapperLogType.Prepare);
            var content = MessagePackSerializer.Deserialize<PrepareLog>(log.content);
            Debug.Assert(content != null);
            Debug.Assert(content.grainID.id.Equals(myID.grainID.id));
            Debug.Assert(!string.IsNullOrEmpty(content.grainID.className));
            Debug.Assert(Helper.GetReplicaClassName(content.grainID.className).Equals(myID.grainID.className));

            var updatesOnDictionary = SnapperSerializer.DeserializeUpdatesOnDictionary(content.updatesOnDictionary);
            var updatesOnReference = SnapperSerializer.DeserializeUpdatesOnReference(content.updatesOnReference);
            var updatesOnList = SnapperSerializer.DeserializeUpdatesOnList(content.updatesOnList);
            await historyManager.RegisterGrainState(myID, content, new SnapperGrainStateUpdates(updatesOnDictionary, updatesOnReference, updatesOnList));
        }
        catch (Exception e)
        {
            Console.WriteLine($"ProcessLog: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }

    /// <summary> execute a read-only ACT </summary>
    async Task<TransactionResult> ITransactionReplicationGrain.ExecuteACT(FunctionCall startFunc)
    {
        try
        {
            // get the transaction context
            var time = DateTime.UtcNow;
            var result = new TransactionResult();
            var tid = await myReplicaRegionalCoordGrain.NewACT();
            var cxt = new TransactionContext(tid);
            result.time.Add(BreakDownLatency.REGISTER, (DateTime.UtcNow - time).TotalMilliseconds);
            time = DateTime.UtcNow;

            // invoke call on the first grain
            var funcResult = await Execute(startFunc, cxt);
            result.time.Add(BreakDownLatency.GETTURN, (funcResult.timeToGetTurn - time).TotalMicroseconds);
            result.time.Add(BreakDownLatency.GETSTATE, (funcResult.timeToGetState - funcResult.timeToGetTurn).TotalMicroseconds);
            result.time.Add(BreakDownLatency.EXECUTE, (funcResult.timeToFinishExecution - funcResult.timeToGetState).TotalMicroseconds);
            result.MergeResult(funcResult);

            // transaction completes, now can release locks on related grains
            var tasks = new List<Task>();
            foreach (var grainID in result.grains)
            {
                (var regionID, _) = grainID.GetLocation();

                Debug.Assert(regionID.Equals(myRegionID));
                var grain = GrainFactory.GetGrain<ITransactionReplicationGrain>(grainID.grainID.id, grainID.location, grainID.grainID.className);
                tasks.Add(grain.Commit(cxt));
            }
            await Task.WhenAll(tasks);
            result.time.Add(BreakDownLatency.PREPARE, (DateTime.UtcNow - funcResult.timeToFinishExecution).TotalMilliseconds);
            time = DateTime.UtcNow;
            result.time.Add(BreakDownLatency.COMMIT, (DateTime.UtcNow - time).TotalMilliseconds);

            result.resultObj = funcResult.resultObj;
            return result;
        }
        catch (Exception e)
        {
            Console.WriteLine($"ExecuteACT: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task<FunctionResult> Execute(FunctionCall call, TransactionContext cxt)
    {
        // invoke the function call on this grain
        var funcResult = new FunctionResult();
        try
        {
            var resultObj = await InvokeFunction(call, cxt);
            funcResult.timeToFinishExecution = DateTime.UtcNow;

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
            Console.WriteLine($"Execute: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
        
        if (cxt.isDet)
        {
            // case 1: the transaction has called other grains
            // case 2: the transaction has tried to RW state
            if (funcResults.ContainsKey(cxt.tid))
            {
                funcResult.MergeResult(funcResults[cxt.tid]);
                funcResult.timeToGetTurn = funcResults[cxt.tid].timeToGetTurn;
                funcResult.timeToGetState = funcResults[cxt.tid].timeToGetState;

                funcResults.Remove(cxt.tid);

                // case 1: exception is from other grains
                // case 2: exception is from RW operation
                if (funcResult.hasException())
                {
                    Console.WriteLine($"TransactionReplicationGrain: Execute, This should never happen");
                    Debug.Assert(false);
                }
                else await historyManager.CompleteTxn(myID, cxt);
            }
            else await historyManager.CompleteTxn(myID, cxt);
        }
        else
        {
            if (funcResults.ContainsKey(cxt.tid))
            {
                funcResult.MergeResult(funcResults[cxt.tid]);
                funcResult.timeToGetTurn = funcResults[cxt.tid].timeToGetTurn;
                funcResult.timeToGetState = funcResults[cxt.tid].timeToGetState;

                // case 1: exception is from other grains
                // case 2: exception is from read operation
                if (funcResult.hasException())
                {
                    if (funcResult.grains.Contains(myID)) funcResult.RemoveGrain(myID);
                    await historyManager.CompleteTxn(myID, cxt);
                    funcResults.Remove(cxt.tid);
                }
                else if (!funcResults[cxt.tid].grains.Contains(myID)) funcResults.Remove(cxt.tid);
            }
        }
        return funcResult;
    }

    async Task<object?> InvokeFunction(FunctionCall call, TransactionContext cxt)
    {
        try
        {
            // check if it's internal functions
            if (call.type == null)
            {
                var func = Enum.Parse<SnapperInternalFunction>(call.funcName);
                switch (func)
                {
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
                var task = method.Invoke(this, new object[] { cxt, call.funcInput }) as Task<object?>;
                if (task == null) throw new Exception($"Fail to invoke the method {call.funcName} on type {call.type.AssemblyQualifiedName}");

                // execute the function call
                return await task;
            }
        }
        catch (SnapperException)
        {
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Grain {myID.grainID.Print()}: InvokeFunction {call.funcName}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task<object?> CallGrain(GrainID grainID, FunctionCall call, TransactionContext cxt)
    {
        if (funcResults.ContainsKey(cxt.tid) && funcResults[cxt.tid].hasException())
        {
            Debug.Assert(funcResults[cxt.tid].exception.HasValue);
            throw new SnapperException(funcResults[cxt.tid].exception.Value);
        }

        // check if the target grain has replica within the silo
        (var hasReplicaInCurrentRegion, var siloID) = snapperReplicaCache.GetReplicaGrainLocation(grainID);
        if (!hasReplicaInCurrentRegion)
        {
            Console.WriteLine($"The grain replica (id = {grainID.Print()}, name = {grainID.className}) is not accessible in region {myRegionID}");
            Debug.Assert(false);
            throw new Exception();
        }

        // check if the current grain is the target grain
        if (!grainID.Equals(myID))
        {
            var grain = GrainFactory.GetGrain<ITransactionReplicationGrain>(grainID.id, myRegionID + "+" + siloID, grainID.className);

            try
            {
                // execute transaction on the target grain
                var funcResult = await grain.Execute(call, cxt);
                
                // merge the function result from the remote grain
                if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, funcResult);
                else funcResults[cxt.tid].MergeResult(funcResult);

                if (funcResult.hasException()) throw new SnapperException(funcResult.exception.Value);
               
                return funcResult.resultObj;
            }
            catch (SnapperException)
            {
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Grain {myID.grainID.Print()}: call grain {grainID.Print()}, tid = {cxt.tid.id}, bid = {cxt.bid.id}, {e.Message}, {e.StackTrace}");
                Debug.Assert(false);
                throw;
            }
        }
        else return await InvokeFunction(call, cxt);
    }

    public async Task<(DictionaryState, ListState)> GetState(TransactionContext cxt)
    {
        try
        {
            if (funcResults.ContainsKey(cxt.tid) && funcResults[cxt.tid].hasException())
            {
                Debug.Assert(funcResults[cxt.tid].exception.HasValue);
                throw new SnapperException(funcResults[cxt.tid].exception.Value);
            }

            var state = await historyManager.WaitForTurn(myID, cxt);
            var timeToGetTurn = DateTime.UtcNow;

            var dictionaryState = new DictionaryState(myID.grainID, ImplementationType.SNAPPERSIMPLE, AccessMode.Read, new Dictionary<ISnapperKey, SnapperValue>(state.dictionary), new Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>(state.references));
            var listState = new ListState(AccessMode.Read, new List<SnapperRecord>(state.list));

            var timeToGetState = DateTime.UtcNow;

            if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
            funcResults[cxt.tid].AddGrain(myID);
            funcResults[cxt.tid].timeToGetTurn = timeToGetTurn;
            funcResults[cxt.tid].timeToGetState = timeToGetState;

            return (dictionaryState, listState);
        }
        catch (SnapperException e)
        {
            Debug.Assert(!cxt.isDet);
            if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
            funcResults[cxt.tid].SetException(e.exceptionType);
            throw new SnapperException(e.exceptionType);
        }
        catch (Exception e)
        {
            // this should never happen
            Console.WriteLine($"GetState: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task Commit(TransactionContext cxt)
    {
        var tid = cxt.tid;
        Debug.Assert(funcResults.ContainsKey(tid));
        Debug.Assert(funcResults[tid].grains.Contains(myID));

        await historyManager.CompleteTxn(myID, cxt);

        funcResults.Remove(tid);
    }

    // ========================================================================================================================================
    public async Task<(byte[], byte[], byte[], byte[])> TakeSnapshotOfGrainState(TransactionContext cxt)
    {
        (var dictionaryState, var listState) = await GetState(cxt);

        // serialize dictionary state
        var item1 = SnapperSerializer.Serialize(dictionaryState.GetAllItems());

        // serialize key references info
        var updateReference = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        var deleteReference = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        var references = dictionaryState.GetAllReferences();
        if (references.ContainsKey(SnapperKeyReferenceType.ReplicateReference)) updateReference = references[SnapperKeyReferenceType.ReplicateReference];
        if (references.ContainsKey(SnapperKeyReferenceType.DeleteReference)) deleteReference = references[SnapperKeyReferenceType.DeleteReference];
        var item2 = SnapperSerializer.Serialize(updateReference);
        var item3 = SnapperSerializer.Serialize(deleteReference);

        // serialize list state
        var item4 = SnapperSerializer.Serialize(listState.GetAllEntries());

        return (item1, item2, item3, item4);
    }
}
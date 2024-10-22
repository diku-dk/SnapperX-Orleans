using Utilities;
using Orleans.Concurrency;
using Concurrency.Implementation.GrainPlacement;
using Orleans.GrainDirectory;
using Concurrency.Common;
using System.Diagnostics;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Concurrency.Interface.DataModel;

namespace Concurrency.Implementation.DataModel;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public abstract class NonTransactionalKeyValueGrain : Grain, INonTransactionalKeyValueGrain
{
    // grain basic info
    readonly string myNameSpace;
    public SnapperGrainID myID;
    string myRegionID;
    string mySiloID;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly ISnapperReplicaCache snapperReplicaCache;

    Dictionary<SnapperID, FunctionResult> funcResults;    // tid, result

    NonTransactionalAPI nonTransactionalAPI;
    NonTransactionalGrainStateManager stateManager;
    NonTransactionalKeyReferenceManager keyReferenceManager;

    public NonTransactionalKeyValueGrain(string? myNameSpace, ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
    {
        Debug.Assert(!string.IsNullOrEmpty(myNameSpace));
        this.myNameSpace = myNameSpace;
        this.snapperClusterCache = snapperClusterCache;
        this.snapperReplicaCache = snapperReplicaCache;
    }

    /// <summary> Any type of message may cause the grain being activated </summary>
    public override Task OnActivateAsync(CancellationToken _)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], myNameSpace);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        funcResults = new Dictionary<SnapperID, FunctionResult>();
        stateManager = new NonTransactionalGrainStateManager(myID);
        nonTransactionalAPI = new NonTransactionalAPI(myID, myRegionID, stateManager, GrainFactory, snapperClusterCache, funcResults);
        keyReferenceManager = new NonTransactionalKeyReferenceManager(myID.grainID, nonTransactionalAPI, stateManager);

        return Task.CompletedTask;
    }

    public Task<AggregatedExperimentData> CheckGC()
    {
        if (funcResults.Count != 0) Console.WriteLine($"TransactionExecutionGrain: funcResults.Count = {funcResults.Count}");
        return Task.FromResult(new AggregatedExperimentData());
    }

    public async Task<FunctionResult> Execute(FunctionCall call, SnapperID tid)
    {
        var funcResult = new FunctionResult();

        try
        {
            // invoke the user specified function
            var resultObj = await InvokeFunction(call, tid);

            // set the result for this function call
            if (resultObj != null) funcResult.SetResult(resultObj);

            funcResult.AddGrain(myID);

            if (funcResults.ContainsKey(tid)) funcResult.MergeResult(funcResults[tid]);
        }
        catch (Exception e)
        {
            //Console.WriteLine($"{e.Message}, {e.StackTrace}");
            funcResult.SetException(ExceptionType.AppAbort);
            throw;
        }
        finally
        {
            if (funcResults.ContainsKey(tid)) funcResults.Remove(tid);
        }

        return funcResult;
    }

    async Task<object?> InvokeFunction(FunctionCall call, SnapperID tid)
    {
        // check if it's internal functions
        if (call.type == null)
        {
            var func = Enum.Parse<SnapperInternalFunction>(call.funcName);
            switch (func)
            {
                case SnapperInternalFunction.NoOp: return null;

                // =================================================================================================
                // only happen on the origin grains (actor-level concurrency control)
                case SnapperInternalFunction.RegisterReference: return keyReferenceManager.RegisterReference(call.funcInput);
                case SnapperInternalFunction.DeRegisterReference: return keyReferenceManager.DeRegisterReference(call.funcInput);

                // =================================================================================================
                // only happen on follower grains (actor-level concurrency control)
                case SnapperInternalFunction.ApplyForwardedKeyOperations: return keyReferenceManager.ApplyForwardedKeyOperations(call.funcInput);

                // =================================================================================================
                case SnapperInternalFunction.ReadState: return TakeSnapshotOfGrainState();

                default: throw new Exception($"The internal function {call.funcName} is not supported. ");
            }
        }
        else
        {
            // resolve the function call
            var method = call.type.GetMethod(call.funcName);
            if (method == null) throw new Exception($"Fail to find the method {call.funcName} on type {call.type.AssemblyQualifiedName}");

            // invoke the function call
            var task = method.Invoke(this, new object[] { tid, call.funcInput }) as Task<object?>;
            if (task == null) throw new Exception($"Fail to invoke the method {call.funcName} on type {call.type.AssemblyQualifiedName}");

            // execute the function call
            var res = await task;

            // check if need to forward updates to followers (only if the current transaction has done writes)
            if (stateManager.IfStateChanged()) await keyReferenceManager.ForwardKeyOperations(tid);

            return res;
        }
    }

    public (IDictionaryState, IListState) GetState(SnapperID tid) => stateManager.GetState();

    public async Task<object?> CallGrain(GrainID grainID, FunctionCall call, SnapperID tid) => await nonTransactionalAPI.CallGrain(grainID, call, tid);

    public async Task<bool> RegisterReference(SnapperID tid, ReferenceInfo referenceInfo, ISnapperValue? value2 = null) => await nonTransactionalAPI.RegisterReference(tid, referenceInfo, value2);

    public (byte[], byte[], byte[], byte[]) TakeSnapshotOfGrainState()
    {
        (var dictionaryState, var listState) = stateManager.GetState();

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
}

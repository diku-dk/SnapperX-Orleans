using Concurrency.Common;
using Concurrency.Interface.TransactionExecution;
using System.Diagnostics;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Utilities;
using Concurrency.Common.Scheduling;

namespace Concurrency.Implementation.TransactionExecution;

internal class TransactionalAPI
{
    readonly SnapperGrainID myID;
    readonly string myRegionID;

    readonly IGrainFactory grainFactory;
    readonly ISnapperClusterCache snapperClusterCache;

    readonly ScheduleList scheduleList;
    readonly GrainStateManager stateManager;
    readonly Dictionary<SnapperID, FunctionResult> funcResults;    // tid, result

    public TransactionalAPI(SnapperGrainID myID, string myRegionID, 
        IGrainFactory grainFactory, ISnapperClusterCache snapperClusterCache,
        ScheduleList scheduleList, GrainStateManager stateManager, Dictionary<SnapperID, FunctionResult> funcResults)
    {
        this.myID = myID;
        this.myRegionID = myRegionID;
        this.grainFactory = grainFactory;
        this.snapperClusterCache = snapperClusterCache;
        this.scheduleList = scheduleList;
        this.stateManager = stateManager;
        this.funcResults = funcResults;
    }

    public async Task<object?> CallGrain(GrainID grainID, FunctionCall call, TransactionContext cxt)
    {
        try
        {
            if (funcResults.ContainsKey(cxt.tid) && funcResults[cxt.tid].hasException())
            {
                var exception = funcResults[cxt.tid].exception;
                Debug.Assert(exception.HasValue);
                throw new SnapperException(exception.Value);
            }

            // the current grain must not be the target grain
            var masterGrainID = snapperClusterCache.GetMasterGrain(grainID);
            Debug.Assert(!masterGrainID.Equals(myID));

            ITransactionExecutionGrain grain;

            (var regionID, _) = masterGrainID.GetLocation();
            if (regionID != myRegionID)
            {
                var client = snapperClusterCache.GetOrleansClient(regionID);
                grain = client.GetGrain<ITransactionExecutionGrain>(masterGrainID.grainID.id, masterGrainID.location, masterGrainID.grainID.className);
            }
            else grain = grainFactory.GetGrain<ITransactionExecutionGrain>(masterGrainID.grainID.id, masterGrainID.location, masterGrainID.grainID.className);

            // execute transaction on the target grain
            var funcResult = await grain.Execute(call, cxt);

            // merge the function result from the remote grain
            if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
            funcResults[cxt.tid].MergeResult(funcResult);
            
            if (funcResults[cxt.tid].hasException())
            {
                var exception = funcResults[cxt.tid].exception;
                Debug.Assert(exception.HasValue);
                throw new SnapperException(exception.Value);
            }

            return funcResult.resultObj;
        }
        catch (SnapperException)
        {
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine($"{myID.grainID.Print()}, invoke {call.funcName} on {grainID.Print()}, bid = {cxt.bid.Print()}, tid = {cxt.tid.Print()}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    /// <summary> no data model + actor-level concurrency control </summary>
    public async Task<(DictionaryState, ListState)> GetInternalSimpleState(AccessMode mode, TransactionContext cxt)
    {
        try
        {
            // we do not allow client code to call no-op
            if (mode == AccessMode.NoOp) throw new Exception($"The access mode NoOp cannot be used externally. ");

            if (funcResults.ContainsKey(cxt.tid) && funcResults[cxt.tid].hasException())
            {
                var exception = funcResults[cxt.tid].exception;
                Debug.Assert(exception.HasValue);
                throw new SnapperException(exception.Value);
            }

            await scheduleList.WaitForScheduleInfo(cxt);
            var timeToGetScheduleReady = DateTime.UtcNow;

            await scheduleList.WaitForTurn(cxt);
            var timeToGetTurn = DateTime.UtcNow;

            (var dictionaryState, var listState) = await stateManager.GetSimpleState(mode, cxt);
            var timeToGetState = DateTime.UtcNow;

            if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
            funcResults[cxt.tid].AddGrain(myID);

            funcResults[cxt.tid].timeToGetScheduleReady = timeToGetScheduleReady;
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
            Console.WriteLine($"grain {myID.grainID.Print()}: fail to GetInternalSimpleState for tid = {cxt.tid.Print()}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    /// <summary> data model + actor-level concurrency control </summary>
    public async Task<(DictionaryState, ListState)> GetInternalKeyValueState(AccessMode mode, TransactionContext cxt, HashSet<ISnapperKey> keys)
    {
        try
        {
            // we do not allow client code to call no-op
            if (mode == AccessMode.NoOp) throw new Exception($"The access mode NoOp cannot be used externally. ");

            if (funcResults.ContainsKey(cxt.tid) && funcResults[cxt.tid].hasException())
            {
                var exception = funcResults[cxt.tid].exception;
                Debug.Assert(exception.HasValue);
                throw new SnapperException(exception.Value);
            }

            await scheduleList.WaitForScheduleInfo(cxt);
            var timeToGetScheduleReady = DateTime.UtcNow;

            await scheduleList.WaitForTurn(cxt);
            var timeToGetTurn = DateTime.UtcNow;

            (var dictionaryState, var listState) = await stateManager.GetKeyValueState(mode, cxt, keys);
            var timeToGetState = DateTime.UtcNow;

            if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
            funcResults[cxt.tid].AddGrain(myID);

            funcResults[cxt.tid].timeToGetScheduleReady = timeToGetScheduleReady;
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
            Console.WriteLine($"{e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    /// <summary> data model + key-level concurrency control </summary>
    public async Task<(DictionaryState, ListState)> GetInternalFineState(TransactionContext cxt, Dictionary<ISnapperKey, AccessMode> keys)
    {
        try
        {
            // we do not allow client code to call no-op
            if (keys.Values.ToHashSet().Contains(AccessMode.NoOp)) throw new Exception($"The access mode NoOp cannot be used externally. ");

            if (funcResults.ContainsKey(cxt.tid) && funcResults[cxt.tid].hasException())
            {
                Debug.Assert(funcResults[cxt.tid].exception.HasValue);
                throw new SnapperException(funcResults[cxt.tid].exception.Value);
            }

            await scheduleList.WaitForScheduleInfo(cxt);
            var timeToGetScheduleReady = DateTime.UtcNow;

            var timeToGetTurn = DateTime.UtcNow;
            var timeToGetState = DateTime.UtcNow;

            DictionaryState dictionaryState;
            ListState listState;
            if (!cxt.isDet)
            {
                await scheduleList.WaitForTurn(cxt);
                timeToGetTurn = DateTime.UtcNow;

                (dictionaryState, listState) = await stateManager.GetFineState(cxt, keys);
                timeToGetState = DateTime.UtcNow;
            }
            else
            {
                var keySet = keys.Keys.ToHashSet();
                await scheduleList.WaitForTurn(cxt, keySet);
                timeToGetTurn = DateTime.UtcNow;

                (dictionaryState, listState) = await stateManager.GetFineState(cxt, keys);
                timeToGetState = DateTime.UtcNow;
            }
            
            if (!funcResults.ContainsKey(cxt.tid)) funcResults.Add(cxt.tid, new FunctionResult());
            funcResults[cxt.tid].AddGrain(myID);

            funcResults[cxt.tid].timeToGetScheduleReady = timeToGetScheduleReady;
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
            Console.WriteLine($"grain {myID.grainID.Print()}: fail to GetInternalKeys for bid = {cxt.bid.Print()}, tid = {cxt.tid.Print()}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task<bool> RegisterReference(ImplementationType implementationType, TransactionContext cxt, ReferenceInfo referenceInfo, ISnapperValue? value2 = null)
    {
        Debug.Assert(referenceInfo != null && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        string funcName;
        DictionaryState dictionaryState;

        switch (implementationType)
        {
            case ImplementationType.SNAPPER:
                funcName = SnapperInternalFunction.RegisterReference.ToString();
                (dictionaryState, _) = await GetInternalKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey>{ referenceInfo.key2 });
                break;
            case ImplementationType.SNAPPERFINE:
                funcName = SnapperInternalFunction.RegisterFineReference.ToString();
                (dictionaryState, _) = await GetInternalFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { referenceInfo.key2, AccessMode.ReadWrite } });
                break;
            default:
                throw new Exception($"The implementationType {implementationType} is not supported. ");
        }

        // STEP 1: check if the current actor state can have the key as a follower
        var succeed = dictionaryState.CheckIfKeyCanBeRegisteredWithReference(referenceInfo.key2);
        if (!succeed && !cxt.isDet) return false;
        
        if (succeed)
        {
            // STEP 2: register reference info on the origin grain
            var call = new FunctionCall(funcName, referenceInfo);
            var res = await CallGrain(referenceInfo.grain1, call, cxt);
            Debug.Assert(res != null);
            (succeed, var value) = ((bool, ISnapperValue?))res;
            if (!succeed) return false;

            // STEP 3: store the key on thelocal actor as a follower key with the regitered reference
            Debug.Assert(value != null);
            if (referenceInfo.referenceType == SnapperKeyReferenceType.ReplicateReference)
            {
                // in this case, the follower has exactly the same key-value record as origin
                Debug.Assert(value2 == null);
                (succeed, var _) = dictionaryState.PutKeyWithReference(referenceInfo, value);
                Debug.Assert(succeed);
                return true;
            }
            else
            {
                // in this case, the follower value does not have to be the same as the origin
                var v = value2 == null ? value : value2;
                (succeed, var _) = dictionaryState.PutKeyWithReference(referenceInfo, v);
                Debug.Assert(succeed);
                return true;
            }
        }
        else
        {
            var call = new FunctionCall(SnapperInternalFunction.NoOp.ToString());
            await CallGrain(referenceInfo.grain1, call, cxt);
            return false;
        }
    }
}
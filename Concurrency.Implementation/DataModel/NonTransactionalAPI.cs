using Concurrency.Common;
using Concurrency.Interface.DataModel;
using System.Diagnostics;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Utilities;

namespace Concurrency.Implementation.DataModel;

internal class NonTransactionalAPI
{
    readonly SnapperGrainID myID;
    readonly string myRegionID;
    readonly NonTransactionalGrainStateManager stateManager;

    readonly IGrainFactory grainFactory;
    readonly ISnapperClusterCache snapperClusterCache;

    readonly Dictionary<SnapperID, FunctionResult> funcResults;    // tid, result

    public NonTransactionalAPI(SnapperGrainID myID, string myRegionID, NonTransactionalGrainStateManager stateManager,
        IGrainFactory grainFactory, ISnapperClusterCache snapperClusterCache, Dictionary<SnapperID, FunctionResult> funcResults)
    {
        this.myID = myID;
        this.myRegionID = myRegionID;
        this.stateManager = stateManager;
        this.grainFactory = grainFactory;
        this.snapperClusterCache = snapperClusterCache;
        this.funcResults = funcResults;
    }

    public async Task<object?> CallGrain(GrainID grainID, FunctionCall call, SnapperID tid)
    {
        // the current grain must not be the target grain
        var masterGrainID = snapperClusterCache.GetMasterGrain(grainID);
        Debug.Assert(!masterGrainID.Equals(myID));
        INonTransactionalKeyValueGrain grain;

        (var regionID, _) = masterGrainID.GetLocation();
        if (regionID != myRegionID)
        {
            var client = snapperClusterCache.GetOrleansClient(regionID);
            grain = client.GetGrain<INonTransactionalKeyValueGrain>(masterGrainID.grainID.id, masterGrainID.location, masterGrainID.grainID.className);
        }
        else grain = grainFactory.GetGrain<INonTransactionalKeyValueGrain>(masterGrainID.grainID.id, masterGrainID.location, masterGrainID.grainID.className);

        // execute transaction on the target grain
        var funcResult = await grain.Execute(call, tid);

        if (!funcResults.ContainsKey(tid)) funcResults.Add(tid, funcResult);
        else funcResults[tid].MergeResult(funcResult);

        return funcResult.resultObj;
    }

    public async Task<bool> RegisterReference(SnapperID tid, ReferenceInfo referenceInfo, ISnapperValue? value2 = null)
    {
        Debug.Assert(referenceInfo != null && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        // STEP 1: check if the current actor state can have the key as a follower
        (var dictionaryState, var _) = stateManager.GetState();
        var succeed = dictionaryState.CheckIfKeyCanBeRegisteredWithReference(referenceInfo.key2);
        if (!succeed) return false;

        // STEP 2: register reference info on the origin grain
        var call = new FunctionCall(SnapperInternalFunction.RegisterReference.ToString(), referenceInfo);
        var res = await CallGrain(referenceInfo.grain1, call, tid);
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
}
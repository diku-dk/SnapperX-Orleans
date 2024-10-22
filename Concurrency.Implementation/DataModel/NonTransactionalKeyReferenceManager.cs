using Concurrency.Common;
using Concurrency.Common.State;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.DataModel;

internal class NonTransactionalKeyReferenceManager
{
    readonly GrainID myID;

    readonly NonTransactionalAPI nonTransactionalAPI;

    readonly NonTransactionalGrainStateManager stateManager;

    public NonTransactionalKeyReferenceManager(GrainID myID, NonTransactionalAPI nonTransactionalAPI, NonTransactionalGrainStateManager stateManager)
    {
        this.myID = myID;
        this.nonTransactionalAPI = nonTransactionalAPI;
        this.stateManager = stateManager;
    }

    public (bool, ISnapperValue?) RegisterReference(object? obj)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null);
        if (!referenceInfo.grain1.Equals(myID) || referenceInfo.referenceType == SnapperKeyReferenceType.NoReference)
            Console.WriteLine($"Grain {myID.Print()}: ref info grain1 = {referenceInfo.grain1.Print()}, type = {referenceInfo.referenceType}");
        Debug.Assert(referenceInfo.grain1.Equals(myID) && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        (var dictionaryState, var _) = stateManager.GetState();
        var value = dictionaryState.GetOrigin(referenceInfo.key1);
        if (value != null) return dictionaryState.RegisterReference(referenceInfo);
        else return (false, null);
    }

    public (bool, ISnapperValue?) DeRegisterReference(object? obj)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null);
        if (!referenceInfo.grain1.Equals(myID) || referenceInfo.referenceType == SnapperKeyReferenceType.NoReference)
            Console.WriteLine($"Grain {myID.Print()}: ref info grain1 = {referenceInfo.grain1.Print()}, type = {referenceInfo.referenceType}");
        Debug.Assert(referenceInfo.grain1.Equals(myID) && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        (var dictionaryState, var _) = stateManager.GetState();
        var value = dictionaryState.GetOrigin(referenceInfo.key1);
        if (value != null)
        {
            dictionaryState.DeRegisterReference(referenceInfo);
            return (true, value);
        }
        else return (false, null);
    }

    /// <summary> forward updates on master keys to all copies </summary>
    public async Task ForwardKeyOperations(SnapperID tid)
    {
        (var dictionaryState, var _) = stateManager.GetState();
        var updatesPerGrain = dictionaryState.GetUpdatesToForward(myID);

        // forward updates to affected grains
        var tasks = new List<Task>();
        foreach (var item in updatesPerGrain)
        {
            var affectedGrain = item.Key;
            var input = (myID, item.Value.Select(SnapperSerializer.Serialize).ToList());
            tasks.Add(nonTransactionalAPI.CallGrain(affectedGrain, new FunctionCall(SnapperInternalFunction.ApplyForwardedKeyOperations.ToString(), input), tid));
        }
        await Task.WhenAll(tasks);
    }

    public string ApplyForwardedKeyOperations(object? obj)
    {
        Debug.Assert(obj != null);
        (var fromGrain, var bytes) = ((GrainID, List<byte[]>))obj;
        var updates = bytes.Select(SnapperSerializer.DeserializeForwardedUpdate).ToList();

        (var dictionaryState, var _) = stateManager.GetState();
        dictionaryState.ApplyForwardedUpdates(fromGrain, updates);
        return $"Finish applying {updates.Count} operations. ";
    }
}

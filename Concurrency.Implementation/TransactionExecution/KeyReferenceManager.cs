using Concurrency.Common.State;
using Concurrency.Common;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution;

internal class KeyReferenceManager
{
    readonly GrainID myID;
    readonly TransactionalAPI transactionalAPI;

    public KeyReferenceManager(GrainID myID,TransactionalAPI transactionalAPI)
    { 
        this.myID = myID;
        this.transactionalAPI = transactionalAPI;
    }

    public async Task<(bool, ISnapperValue?)> RegisterReference(ImplementationType implementationType, TransactionContext cxt, object? obj)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null);
        if (!referenceInfo.grain1.Equals(myID) || referenceInfo.referenceType == SnapperKeyReferenceType.NoReference)
            Console.WriteLine($"Grain {myID.Print()}: ref info grain1 = {referenceInfo.grain1.Print()}, type = {referenceInfo.referenceType}");
        Debug.Assert(referenceInfo.grain1.Equals(myID) && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        DictionaryState dictionaryState;
        switch (implementationType)
        {
            case ImplementationType.SNAPPERSIMPLE:
                (dictionaryState, _) = await transactionalAPI.GetInternalSimpleState(AccessMode.ReadWrite, cxt);
                break;
            case ImplementationType.SNAPPER:
                (dictionaryState, _) = await transactionalAPI.GetInternalKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey> { referenceInfo.key1 });
                break;
            case ImplementationType.SNAPPERFINE:
                (dictionaryState, _) = await transactionalAPI.GetInternalFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { referenceInfo.key1, AccessMode.ReadWrite } });
                break;
            default:
                Debug.Assert(false);
                throw new Exception($"The implementationType {implementationType} is not supported. ");
        }

        var value = dictionaryState.GetOrigin(referenceInfo.key1);
        if (value != null) return dictionaryState.RegisterReference(referenceInfo);
        else return (false, null);
    }

    public async Task<(bool, ISnapperValue?)> DeRegisterReference(ImplementationType implementationType, TransactionContext cxt, object? obj)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null);
        if (!referenceInfo.grain1.Equals(myID) || referenceInfo.referenceType == SnapperKeyReferenceType.NoReference)
            Console.WriteLine($"Grain {myID.Print()}: ref info grain1 = {referenceInfo.grain1.Print()}, type = {referenceInfo.referenceType}");
        Debug.Assert(referenceInfo.grain1.Equals(myID) && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        DictionaryState dictionaryState;
        switch (implementationType)
        {
            case ImplementationType.SNAPPERSIMPLE:
                (dictionaryState, _) = await transactionalAPI.GetInternalSimpleState(AccessMode.ReadWrite, cxt);
                break;
            case ImplementationType.SNAPPER:
                (dictionaryState, _) = await transactionalAPI.GetInternalKeyValueState(AccessMode.ReadWrite, cxt, new HashSet<ISnapperKey> { referenceInfo.key1 });
                break;
            case ImplementationType.SNAPPERFINE:
                (dictionaryState, _) = await transactionalAPI.GetInternalFineState(cxt, new Dictionary<ISnapperKey, AccessMode> { { referenceInfo.key1, AccessMode.ReadWrite } });
                break;
            default:
                Debug.Assert(false);
                throw new Exception($"The implementationType {implementationType} is not supported. ");
        }

        var value = dictionaryState.GetOrigin(referenceInfo.key1);
        if (value != null)
        {
            dictionaryState.DeRegisterReference(referenceInfo);
            return (true, value);
        }
        else return (false, null);
    }

    /// <summary> forward updates on master keys to all copies </summary>
    public async Task ForwardKeyOperations(ImplementationType implementationType, TransactionContext cxt, Dictionary<GrainID, List<ForwardedUpdate>> updatesPerGrain)
    {
        string funcName;
        switch (implementationType)
        {
            case ImplementationType.SNAPPER: funcName = SnapperInternalFunction.ApplyForwardedKeyOperations.ToString(); break;
            case ImplementationType.SNAPPERFINE: funcName = SnapperInternalFunction.ApplyForwardedFineKeyOperations.ToString(); break;
            default:
                Debug.Assert(false);
                throw new Exception($"The implementationType {implementationType} is not supported. ");
        }

        // forward updates to affected grains
        var tasks = new List<Task>();
        foreach (var item in updatesPerGrain)
        {
            var affectedGrain = item.Key;
            var input = (myID, item.Value.Select(SnapperSerializer.Serialize).ToList());
            tasks.Add(transactionalAPI.CallGrain(affectedGrain, new FunctionCall(funcName, input), cxt));
        }
        await Task.WhenAll(tasks);
    }

    public async Task<string> ApplyForwardedKeyOperations(ImplementationType implementationType, TransactionContext cxt, object? obj)
    {
        Debug.Assert(obj != null);
        (var fromGrain, var bytes) = ((GrainID, List<byte[]>))obj;
        var updates = bytes.Select(SnapperSerializer.DeserializeForwardedUpdate).ToList();
        Debug.Assert(updates.Count != 0);

        var affectedKeys = updates.Select(x => x.affectedKey).ToHashSet().Select(x => new KeyValuePair<ISnapperKey, AccessMode>(x, AccessMode.ReadWrite)).ToDictionary();

        DictionaryState dictionaryState;
        switch (implementationType)
        {
            case ImplementationType.SNAPPER:
                (dictionaryState, _) = await transactionalAPI.GetInternalKeyValueState(AccessMode.ReadWrite, cxt, affectedKeys.Keys.ToHashSet());
                break;
            case ImplementationType.SNAPPERFINE:
                (dictionaryState, _) = await transactionalAPI.GetInternalFineState(cxt, affectedKeys);
                break;
            default:
                Debug.Assert(false);
                throw new Exception($"The implementationType {implementationType} is not supported. ");
        }

        dictionaryState.ApplyForwardedUpdates(fromGrain, updates);
        return $"Finish applying {updates.Count} operations. ";
    }
}
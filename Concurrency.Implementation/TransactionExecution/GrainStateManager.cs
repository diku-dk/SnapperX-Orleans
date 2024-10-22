using Concurrency.Common;
using Concurrency.Common.State;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution;

internal class GrainStateManager
{ 
    readonly SnapperGrainID myGrainID;

    SnapperReadWriteLockWaitDie actLock;

    /// <summary> the latest committed version of grain state </summary>
    SnapperGrainState state;

    /// <summary> 
    /// tid
    /// reader or writer
    /// a copy of the latest dictionary state
    /// a copy of the latest list state
    /// an immutable snapshot of the version read by the transaction
    /// </summary>
    Dictionary<SnapperID, (ImplementationType, DictionaryState, ListState)> states;

    /// <summary> 
    /// primary bid
    /// the updates applied to the dictionary state
    /// the appened records written by the batch
    /// </summary>
    Dictionary<SnapperID, SnapperGrainStateUpdates> writtenStatePerBatch;

    // ======================================================================== for key-level concurrency control

    /// <summary> key, pack lock, actor lock </summary>
    Dictionary<ISnapperKey, SnapperReadWriteLockWaitDie> lockPerKey;

    public GrainStateManager(SnapperGrainID myGrainID)
    {
        this.myGrainID = myGrainID;
        state = new SnapperGrainState(myGrainID.grainID);
        actLock = new SnapperReadWriteLockWaitDie(myGrainID);
        states = new Dictionary<SnapperID, (ImplementationType, DictionaryState, ListState)>();
        writtenStatePerBatch = new Dictionary<SnapperID, SnapperGrainStateUpdates>();
        lockPerKey = new Dictionary<ISnapperKey, SnapperReadWriteLockWaitDie>();
    }

    public void CheckGC()
    {
        actLock.CheckGC();
        if (states.Count != 0) Console.WriteLine($"GrainStateManager: states.Count = {states.Count}");
        if (writtenStatePerBatch.Count != 0) Console.WriteLine($"GrainStateManager: writtenStatePerBatch.Count = {writtenStatePerBatch.Count}");
        foreach (var item in lockPerKey) item.Value.CheckGC();
       
        if (lockPerKey.Count > state.dictionary.Count) Console.WriteLine($"GrainStateManager of grain {myGrainID.grainID.Print()}: {lockPerKey.Count - state.dictionary.Count} useless lock");
    }

    public async Task<(DictionaryState, ListState)> GetSimpleState(AccessMode mode, TransactionContext cxt)
    {
        if (states.ContainsKey(cxt.tid)) return (states[cxt.tid].Item2, states[cxt.tid].Item3);

        DictionaryState dictionaryState;
        ListState listState;
        if (!cxt.isDet)
        {
            await actLock.AcquireLock(mode, cxt);

            var clonedState = state.Clone() as SnapperGrainState;
            Debug.Assert(clonedState != null);

            dictionaryState = new DictionaryState(myGrainID.grainID, ImplementationType.SNAPPERSIMPLE, mode, clonedState.dictionary, clonedState.references);
            listState = new ListState(mode, clonedState.list);
        }
        else
        {
            dictionaryState = new DictionaryState(myGrainID.grainID, ImplementationType.SNAPPERSIMPLE, mode, state.dictionary, state.references);
            listState = new ListState(mode, state.list);
        }

        states.Add(cxt.tid, (ImplementationType.SNAPPERSIMPLE, dictionaryState, listState));
        return (dictionaryState, listState);
    }

    public async Task<(DictionaryState, ListState)> GetKeyValueState(AccessMode mode, TransactionContext cxt, HashSet<ISnapperKey> keys)
    {
        // =======================================================================================
        // If it does read / write before
        if (states.ContainsKey(cxt.tid))
        {
            // the previous one is a R, now try to do RW, we do not support lock upgrade, throw exception directly
            if (mode == AccessMode.ReadWrite && states[cxt.tid].Item2.accessMode == AccessMode.Read)
                throw new SnapperException(ExceptionType.RWConflict);

            // check if need to get access to more keys
            //Debug.Assert(states[cxt.tid].Item1 == ImplementationType.SNAPPER);
            foreach (var key in keys)
            {
                var existingItems = states[cxt.tid].Item2.GetAllItems();
                if (!existingItems.ContainsKey(key))
                {
                    if (!state.dictionary.ContainsKey(key)) continue;

                    var copiedValue = state.dictionary[key].Clone() as SnapperValue;
                    Debug.Assert(copiedValue != null);
                    existingItems.Add(key, copiedValue);
                }
            }

            return (states[cxt.tid].Item2, states[cxt.tid].Item3);
        }

        // =======================================================================================
        // If it is the first read / write operation
        if (!cxt.isDet) await actLock.AcquireLock(mode, cxt);

        var list = new List<SnapperRecord>();
        (var dictionary, var references) = state.CloneInfoOfKeys(keys);

        var copyDictionaryState = new DictionaryState(myGrainID.grainID, ImplementationType.SNAPPER, mode, dictionary, references);
        var copyListState = new ListState(mode, list);

        states.Add(cxt.tid, (ImplementationType.SNAPPER, copyDictionaryState, copyListState));

        return (copyDictionaryState, copyListState);
    }

    /// <summary> use this method to acquire locks on the specified keys </summary>
    /// <returns> the dictionary only contains keys that are required </returns>
    public async Task<(DictionaryState, ListState)> GetFineState(TransactionContext cxt, Dictionary<ISnapperKey, AccessMode> keys)
    {
        // =======================================================================================
        // If it does read / write before
        if (states.ContainsKey(cxt.tid))
        {
            // check if need lock more keys
            var moreKeysToLock = new Dictionary<ISnapperKey, AccessMode>();
            foreach (var item in keys)
            {
                // this transaction did not require the lock of the key before
                if (!states[cxt.tid].Item2.accessModePerKey.ContainsKey(item.Key))
                {
                    // the dictionary version that this transaction is working on now must not contain this key yet
                    //Debug.Assert(!states[cxt.tid].Item2.GetAllItems().ContainsKey(item.Key));
                    moreKeysToLock.Add(item.Key, item.Value);
                }
                else
                {
                    // this transaction already hols a lock of the key, but now is asking for lock upgrading, which is not allowed
                    if (item.Value == AccessMode.ReadWrite && states[cxt.tid].Item2.accessModePerKey[item.Key] == AccessMode.Read) 
                        throw new SnapperException(ExceptionType.RWConflict);
                }
            }

            // acquire the lock
            if (!cxt.isDet) await LockKeys(cxt, moreKeysToLock);
            
            // add the keys to this transaction's dictionary version
            foreach (var key in moreKeysToLock)
            {
                // this key might have been deleted by the previous trasnaction
                if (!state.dictionary.ContainsKey(key.Key)) continue;

                var value = state.dictionary[key.Key].Clone() as SnapperValue;
                Debug.Assert(value != null);
                states[cxt.tid].Item2.Put(key.Key, value, key.Value);
            }
            
            return (states[cxt.tid].Item2, states[cxt.tid].Item3);
        }

        // =======================================================================================
        // If it is the first read / write operation

        if (!cxt.isDet) await LockKeys(cxt, keys);
        
        (var dictionaryOfKeys, var referenceOfKeys) = state.CloneInfoOfKeys(keys.Keys.ToHashSet());

        var listCopy = SnapperGrainState.CloneListState(state.list);

        var dictionaryState = new DictionaryState(myGrainID.grainID, keys, dictionaryOfKeys, referenceOfKeys);
        var copyListState = new ListState(AccessMode.ReadWrite, state.list);

        states.Add(cxt.tid, (ImplementationType.SNAPPERFINE, dictionaryState, copyListState));

        return (dictionaryState, copyListState);
    }

    /// <summary> acquire locks for keys </summary>
    /// <returns> the set of keys that actually get the lock </returns>
    async Task LockKeys(TransactionContext cxt, Dictionary<ISnapperKey, AccessMode> keys)
    {
        var locksToAcquire = new  List<(AccessMode, SnapperReadWriteLockWaitDie, ISnapperKey)>();
        foreach ((var key, var mode) in keys)
        {
            if (!lockPerKey.ContainsKey(key)) lockPerKey.Add(key, new SnapperReadWriteLockWaitDie(myGrainID));

            locksToAcquire.Add((mode, lockPerKey[key], key));
        }

        try
        {
            var tasks = new List<Task>();
            foreach (var item in locksToAcquire) tasks.Add(item.Item2.AcquireLock(item.Item1, cxt));
            await Task.WhenAll(tasks);
        }
        catch (SnapperException e)
        {
            Debug.Assert(e.exceptionType == ExceptionType.RWConflict);
            foreach (var item in locksToAcquire)
            {
                item.Item2.ReleaseLock(cxt.tid);

                if (!state.dictionary.ContainsKey(item.Item3) && item.Item2.IsEmpty()) lockPerKey.Remove(item.Item3);
            }
            throw;
        }
    }

    public Dictionary<GrainID, List<ForwardedUpdate>> GetUpdatesToForward(SnapperID tid)
    {
        if (!states.ContainsKey(tid)) return new Dictionary<GrainID, List<ForwardedUpdate>>();
        return states[tid].Item2.GetUpdatesToForward(myGrainID.grainID);
    }
    
    public bool IfLockGranted(SnapperID tid)
    {
        if (!states.ContainsKey(tid)) return false;
        
        switch (states[tid].Item1)
        {
            case ImplementationType.SNAPPERSIMPLE:
            case ImplementationType.SNAPPER:
                return true;
            case ImplementationType.SNAPPERFINE:
                return states[tid].Item2.accessModePerKey.Count != 0;
            default:
                throw new Exception($"This should never happen. ");
        }
    }

    // ======================================================================================================================================= For PACT ====
    /// <summary> called when a batch of PACT has completed the execution on the grain </summary>
    public void SetState(TransactionContext cxt)
    {
        if (!IfStateChanged(cxt.tid)) return;

        if (states[cxt.tid].Item1 == ImplementationType.SNAPPERSIMPLE) return;
     
        // the transaction has done updated on the actor
        (var updatesOnDictionary, var updatesOnReference) = states[cxt.tid].Item2.GetUpdates();
        var updatesOnList = states[cxt.tid].Item3.GetUpdates();
        state.ApplyUpdates(updatesOnDictionary, updatesOnReference, updatesOnList);

        // merge updates from a PACT to the corresponding batch
        if (!writtenStatePerBatch.ContainsKey(cxt.bid)) writtenStatePerBatch[cxt.bid] = new SnapperGrainStateUpdates();
        writtenStatePerBatch[cxt.bid].MergeUpdates(updatesOnDictionary, updatesOnReference, updatesOnList);
    }

    /// <summary> take a snapshot of all updates performed by a batch </summary>
    /// <return> updates on dictionary, updates on log, updates on references </return>
    public (byte[]?, byte[]?, byte[]?) TakeSnapshot(bool isDet, SnapperID bid, SnapperID tid, ImplementationType implementationType)
    {
        if (isDet)
        {
            if (implementationType == ImplementationType.SNAPPERSIMPLE)
            {
                if (!states.ContainsKey(tid) || states[tid].Item2.accessMode == AccessMode.Read) return (null, null, null);

                var serDictionary = SnapperSerializer.Serialize(state.dictionary);
                var serReferences = SnapperSerializer.Serialize(state.references);
                var serList = SnapperSerializer.Serialize(state.list);
                return (serDictionary, serReferences, serList);
            }

            // this state contains all updates happened so far
            if (writtenStatePerBatch.ContainsKey(bid))
            {
                var copy = writtenStatePerBatch[bid];
                if (copy.updatesOnDictionary.Count == 0 && copy.updatesOnReference.Count == 0 && copy.updatesOnList.Count == 0) return (null, null, null);

                var serUpdatesOnDictionary = SnapperSerializer.Serialize(copy.updatesOnDictionary);
                var serUpdatesOnReference = SnapperSerializer.Serialize(copy.updatesOnReference);
                var serUpdatesOnList = SnapperSerializer.Serialize(copy.updatesOnList);

                return (serUpdatesOnDictionary, serUpdatesOnReference, serUpdatesOnList);
            }
            else return (null, null, null);
        }
        else
        {
            if (implementationType == ImplementationType.SNAPPERSIMPLE)
            {
                if (states[tid].Item2.accessMode == AccessMode.Read) return (null, null, null);

                var serDictionary = SnapperSerializer.Serialize(states[tid].Item2.GetAllItems());
                var serReferences = SnapperSerializer.Serialize(states[tid].Item2.GetAllReferences());
                var serList = SnapperSerializer.Serialize(states[tid].Item3.GetAllEntries());
                return (serDictionary, serReferences, serList);
            }

            (var updatesOnDictionary, var updatesOnReferences) = states[tid].Item2.GetUpdates();
            var updatesOnLog = states[tid].Item3.GetUpdates();
            if (updatesOnDictionary.Count == 0 && updatesOnReferences.Count == 0 && updatesOnLog.Count == 0) return (null, null, null);

            var serUpdatesOnDictionary = SnapperSerializer.Serialize(updatesOnDictionary);
            var serUpdatesOnReference = SnapperSerializer.Serialize(updatesOnReferences);
            var serUpdatesOnList = SnapperSerializer.Serialize(updatesOnLog);
            return (serUpdatesOnDictionary, serUpdatesOnReference, serUpdatesOnList);
        }
    }

    /// <returns> if the transaction has made any changes / updates to the grain state </returns>
    public bool IfStateChanged(SnapperID tid)
    {
        if (!states.ContainsKey(tid)) return false;

        if (states[tid].Item1 == ImplementationType.SNAPPERSIMPLE) return states[tid].Item2.accessMode == AccessMode.ReadWrite;

        (var updatesOnDictionary, var updatesOnReference) = states[tid].Item2.GetUpdates();
        var updatesOnList = states[tid].Item3.GetUpdates();

        return updatesOnDictionary.Count != 0 || updatesOnReference.Count != 0 || updatesOnList.Count != 0;
    }

    // ======================================================================================================================================= For ACT ====
    /// <summary> called when the grain receives commit message of an ACT, and the ACT wrote the grain state </summary>
    public void SetState(SnapperID tid)
    {
        if (states[tid].Item1 == ImplementationType.SNAPPERSIMPLE)
        {
            state.dictionary = states[tid].Item2.GetAllItems();
            state.references = states[tid].Item2.GetAllReferences();
            state.list = states[tid].Item3.GetAllEntries();
            return;
        }

        // apply updates of this ACT to state
        var listStateUpdates = states[tid].Item3.GetUpdates();
        (var updatesOnDictionary, var updatesOnReferences) = states[tid].Item2.GetUpdates();
        state.ApplyUpdates(updatesOnDictionary, updatesOnReferences, listStateUpdates);
    }

    public void DeleteState(TransactionContext cxt)
    { 
        if (writtenStatePerBatch.ContainsKey(cxt.bid)) writtenStatePerBatch.Remove(cxt.bid);
    }

    public void DeleteState(SnapperID tid)
    {
        if (states.ContainsKey(tid)) states.Remove(tid);
    }

    public void DeleteStateAndReleaseLock(SnapperID tid)
    {
        if (!states.ContainsKey(tid)) return;

        // release lock
        var implementationType = states[tid].Item1;
        switch (implementationType)
        {
            case ImplementationType.SNAPPERSIMPLE:
            case ImplementationType.SNAPPER:
                actLock.ReleaseLock(tid);
                break;
            case ImplementationType.SNAPPERFINE:
                var dictionary = states[tid].Item2.GetAllItems();
                var lockedKeys = states[tid].Item2.accessModePerKey.Keys.ToList();

                foreach (var key in lockedKeys)
                {
                    if (!lockPerKey.ContainsKey(key)) continue;

                    var actLock = lockPerKey[key];
                    actLock.ReleaseLock(tid);
                    if (!dictionary.ContainsKey(key) && actLock.IsEmpty()) lockPerKey.Remove(key);
                }

                break;
        }

        // remove state
        states.Remove(tid);
    }

    public ImplementationType GetType(SnapperID tid)
    {
        if (states.ContainsKey(tid)) return states[tid].Item1;

        var grainName = myGrainID.grainID.className;
        Debug.Assert(!string.IsNullOrEmpty(grainName));

        if (grainName.Contains("Simple")) return ImplementationType.SNAPPERSIMPLE;
        else if (grainName.Contains("KeyValue")) return ImplementationType.SNAPPER;
        else if (grainName.Contains("Fine")) return ImplementationType.SNAPPERFINE;
        else throw new Exception($"The implementation type of {grainName} is not supported. ");
    }
   
    // ===================================================================================== for grain migration

    /// <summary> set the initial state after grain migration </summary>
    public void SetInitialState(
        Dictionary<ISnapperKey, SnapperValue> dictionary,
        Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> references,
        List<SnapperRecord> list)
        => state.SetInitialState(dictionary, references, list);

    public (Dictionary<ISnapperKey, SnapperValue>, Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>, List<SnapperRecord>) GetState() => state.GetState();
}
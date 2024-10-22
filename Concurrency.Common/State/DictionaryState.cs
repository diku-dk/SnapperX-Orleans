using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.State;

public enum UpdateType { Add, Delete, Modify };

public class DictionaryState : IDictionaryState
{
    public readonly GrainID myID;

    public readonly ImplementationType implementationType;

    /// <summary> this is the access mode declared for the whole state </summary>
    public readonly AccessMode accessMode;

    /// <summary> key, value </summary>
    readonly Dictionary<ISnapperKey, SnapperValue> items;

    /// <summary> 
    /// keep record of the list of updates applied on the dictionary 
    /// TODO: merge multiple updates into one operation !!!!!!!!!!!!!!!!!!!
    /// </summary>
    readonly List<Update> updates;

    /// <summary> reference type, master key, follower grain, follower key, function name (only used for ) </summary>
    readonly Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> references;

    /// <summary> keep record of the list of updates on reference info </summary>
    readonly List<(UpdateType, ReferenceInfo)> updatesOnReference;

    // ===================================================================== for key-level concurrency control

    /// <summary> the declared aceess mode for each key </summary>
    public readonly Dictionary<ISnapperKey, AccessMode> accessModePerKey;

    public DictionaryState(GrainID grainID, ImplementationType implementationType, AccessMode accessMode, Dictionary<ISnapperKey, SnapperValue> existingItems, Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> existingReferences)
    {
        myID = grainID;
        this.implementationType = implementationType;
        Debug.Assert(accessMode != AccessMode.NoOp);
        this.accessMode = accessMode;
        items = existingItems;
        updates = new List<Update>();
        references = existingReferences;
        updatesOnReference = new List<(UpdateType, ReferenceInfo)>();
        accessModePerKey = new Dictionary<ISnapperKey, AccessMode>();
    }

    public DictionaryState(GrainID grainID, Dictionary<ISnapperKey, AccessMode> accessModePerKey, Dictionary<ISnapperKey, SnapperValue> existingItems, Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> existingReferences)
    {
        myID = grainID;
        implementationType = ImplementationType.SNAPPERFINE;
        accessMode = AccessMode.NoOp;
        items = existingItems;
        updates = new List<Update>();
        references = existingReferences;
        updatesOnReference = new List<(UpdateType, ReferenceInfo)>();
        this.accessModePerKey = accessModePerKey;
    }

    /// <summary> this is called only by the grain state manager </summary>
    public void Put(ISnapperKey key, SnapperValue value, AccessMode mode)
    {
        Debug.Assert(!items.ContainsKey(key));
        items.Add(key, value);
        accessModePerKey.Add(key, mode);
    }

    public (List<Update>, List<(UpdateType, ReferenceInfo)>) GetUpdates() => (updates, updatesOnReference);

    public Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>> GetAllReferences() => references;

    // ================================================================================ interfaces that are accessible for users =====================
    
    /// <summary> this interface is only allowed for actor-level concurrency control </summary>
    public Dictionary<ISnapperKey, SnapperValue> GetAllItems() => items;
   
    public ISnapperValue? Get(ISnapperKey key)
    {
        if (accessMode == AccessMode.NoOp) Debug.Assert(accessModePerKey.ContainsKey(key));

        if (!items.ContainsKey(key)) return null;

        if (implementationType == ImplementationType.SNAPPERSIMPLE) return items[key].value;
        else return (ISnapperValue)items[key].value.Clone();
    }

    public (bool, string) Put(ISnapperKey key, ISnapperValue value)
    {
        if (accessMode == AccessMode.NoOp)
        {
            // in this case, the key-level concurrency control is enabled

            var keyTypeName = key.GetAssemblyQualifiedName();
            Debug.Assert(!string.IsNullOrEmpty(keyTypeName));
            if (items.ContainsKey(key))
            {
                // check the allowed access mode on this key
                Debug.Assert(accessModePerKey.ContainsKey(key));
                Debug.Assert(accessModePerKey[key] == AccessMode.ReadWrite);

                var existingValue = items[key];
                var oldValue = existingValue.value;
                Debug.Assert(oldValue != null);

                switch (existingValue.referenceType)
                {
                    case SnapperKeyReferenceType.ReplicateReference:
                        return (false, $"Fail to modify key {key.Print()}, because it has update reference. ");
                    case SnapperKeyReferenceType.NoReference:
                    case SnapperKeyReferenceType.DeleteReference:
                        items[key] = new SnapperValue(value, keyTypeName, existingValue.referenceType, existingValue.function, existingValue.dependentGrain, existingValue.dependentKey);
                        updates.Add(new Update(false, UpdateType.Modify, key, items[key], oldValue));
                        break;
                    default:
                        throw new Exception($"The reference type {existingValue.referenceType} is not supported. ");
                }
                return (true, "");
            }
            else
            {
                //if (!accessModePerKey.ContainsKey(key)) accessModePerKey.Add(key, AccessMode.ReadWrite);
                //else Debug.Assert(accessModePerKey[key] == AccessMode.ReadWrite);
                Debug.Assert(accessModePerKey[key] == AccessMode.ReadWrite);

                items.Add(key, new SnapperValue(value, keyTypeName, SnapperKeyReferenceType.NoReference));
                updates.Add(new Update(false, UpdateType.Add, key, items[key]));

                return (true, "");
            }
        }
        else
        {
            // in this case, the actor-level concurrency control is used

            // check the allowed access mode on this actor
            Debug.Assert(accessMode == AccessMode.ReadWrite);

            var keyTypeName = key.GetAssemblyQualifiedName();
            Debug.Assert(!string.IsNullOrEmpty(keyTypeName));
            if (items.ContainsKey(key))
            {
                var existingValue = items[key];
                var oldValue = existingValue.value;
                Debug.Assert(oldValue != null);

                switch (existingValue.referenceType)
                {
                    case SnapperKeyReferenceType.ReplicateReference:
                        return (false, $"Fail to modify key {key.Print()}, because it has update reference. ");
                    case SnapperKeyReferenceType.NoReference:
                    case SnapperKeyReferenceType.DeleteReference:
                        items[key] = new SnapperValue(value, keyTypeName, existingValue.referenceType, existingValue.function, existingValue.dependentGrain, existingValue.dependentKey);
                        updates.Add(new Update(false, UpdateType.Modify, key, items[key], oldValue));
                        break;
                    default:
                        throw new Exception($"The reference type {existingValue.referenceType} is not supported. ");
                }
                return (true, "");
            }
            else
            {
                items.Add(key, new SnapperValue(value, keyTypeName, SnapperKeyReferenceType.NoReference));
                updates.Add(new Update(false, UpdateType.Add, key, items[key]));

                return (true, "");
            }
        }
    }

    public (bool, string) Delete(ISnapperKey key)
    {
        if (accessMode == AccessMode.NoOp)
        {
            // in this case, the key-level concurrency control is enabled

            // check the allowed access mode on this key
            Debug.Assert(accessModePerKey.ContainsKey(key));
            Debug.Assert(accessModePerKey[key] == AccessMode.ReadWrite);

            if (!items.ContainsKey(key)) return (false, $"Fail to remove key, because it doesn't exist. ");

            updates.Add(new Update(false, UpdateType.Delete, key, items[key]));
            items.Remove(key);

            return (true, "");
        }
        else
        {
            // in this case, the actor-level concurrency control is used

            // check the allowed access mode on this actor
            Debug.Assert(accessMode == AccessMode.ReadWrite);

            if (!items.ContainsKey(key)) return (false, $"Fail to remove key, because it doesn't exist. ");

            updates.Add(new Update(false, UpdateType.Delete, key, items[key]));
            items.Remove(key);

            return (true, "");
        }
    }

    // ============================================================================================================ for registering key reference ================
    /// <summary> check if the key itself is not a follower </summary>
    public ISnapperValue? GetOrigin(ISnapperKey key)
    {
        if (accessMode == AccessMode.NoOp) Debug.Assert(accessModePerKey.ContainsKey(key));

        if (!items.ContainsKey(key)) return null;

        var value = items[key];
        if (value.referenceType != SnapperKeyReferenceType.NoReference) return null;
        Debug.Assert(value.dependentGrain == null && value.dependentKey == null);
        return (ISnapperValue)value.value.Clone();
    }

    /// <summary> check if the key is able to be turned into a follower </summary>
    public bool CheckIfKeyCanBeRegisteredWithReference(ISnapperKey key)
    {
        if (accessMode == AccessMode.NoOp) Debug.Assert(accessModePerKey.ContainsKey(key));

        if (!items.ContainsKey(key)) return true;

        var value = items[key];
        if (value.referenceType != SnapperKeyReferenceType.NoReference) return false;   // if the key already has a reference to another grain, cannot be turned into a new follower

        foreach (var item in references) if (item.Value.ContainsKey(key)) return false; // if the key has followers, cannot be turned into a follower

        return true;
    }

    public (bool, ISnapperValue?) RegisterReference(ReferenceInfo info)
    {
        if (accessMode == AccessMode.NoOp)
        {
            // in this case, the key-level concurrency control is enabled
            Debug.Assert(accessModePerKey.ContainsKey(info.key1));
            Debug.Assert(accessModePerKey[info.key1] == AccessMode.ReadWrite);
        }
        else Debug.Assert(accessMode == AccessMode.ReadWrite);

        if (!items.ContainsKey(info.key1)) return (false, null);
        if (items[info.key1].referenceType != SnapperKeyReferenceType.NoReference) return (false, null);

        // register the reference
        if (!references.ContainsKey(info.referenceType)) references.Add(info.referenceType, new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>());
        if (!references[info.referenceType].ContainsKey(info.key1)) references[info.referenceType].Add(info.key1, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());
        if (!references[info.referenceType][info.key1].ContainsKey(info.grain2)) references[info.referenceType][info.key1].Add(info.grain2, new Dictionary<ISnapperKey, IUpdateFunction>());
        if (!references[info.referenceType][info.key1][info.grain2].ContainsKey(info.key2))
        {
            references[info.referenceType][info.key1][info.grain2].Add(info.key2, info.function);
            updatesOnReference.Add((UpdateType.Add, info));
            return (true, items[info.key1].value);
        }
        else return (false, null);
    }

    public void DeRegisterReference(ReferenceInfo info)
    {
        if (accessMode == AccessMode.NoOp)
        {
            // in this case, the key-level concurrency control is enabled
            Debug.Assert(accessModePerKey.ContainsKey(info.key1));
            Debug.Assert(accessModePerKey[info.key1] == AccessMode.ReadWrite);
        }
        else Debug.Assert(accessMode == AccessMode.ReadWrite);

        if (!items.ContainsKey(info.key1)) return;
        Debug.Assert(items[info.key1].referenceType == SnapperKeyReferenceType.NoReference);

        if (references.ContainsKey(info.referenceType) &&
            references[info.referenceType].ContainsKey(info.key1) &&
            references[info.referenceType][info.key1].ContainsKey(info.grain2) &&
            references[info.referenceType][info.key1][info.grain2].ContainsKey(info.key2))
        {
            references[info.referenceType][info.key1][info.grain2].Remove(info.key2);
            updatesOnReference.Add((UpdateType.Delete, info));
        }
    }

    public (bool, string) PutKeyWithReference(ReferenceInfo info, ISnapperValue value)
    {
        if (info.referenceType == SnapperKeyReferenceType.ReplicateReference) Debug.Assert(info.key1.Equals(info.key2));

        if (accessMode == AccessMode.NoOp)
        {
            // in this case, the key-level concurrency control is enabled

            var key2TypeName = info.key2.GetAssemblyQualifiedName();
            Debug.Assert(!string.IsNullOrEmpty(key2TypeName));
            var newValue = new SnapperValue(value, key2TypeName, info.referenceType, info.function, info.grain1, info.key1);

            if (items.ContainsKey(info.key2))
            {
                // check the allowed access mode on this key
                Debug.Assert(accessModePerKey.ContainsKey(info.key2));
                Debug.Assert(accessModePerKey[info.key2] == AccessMode.ReadWrite);

                var existingValue = items[info.key2];
                var oldValue = existingValue.value;
                Debug.Assert(oldValue != null);
                var existingReferenceType = existingValue.referenceType;

                switch (existingReferenceType)
                {
                    case SnapperKeyReferenceType.NoReference:
                        // if key2 is origin of other keys, cannot convert key2 to a follower
                        if (references.ContainsKey(SnapperKeyReferenceType.ReplicateReference) && references[SnapperKeyReferenceType.ReplicateReference].ContainsKey(info.key2))
                            return (false, $"Fail to PutKeyWithReference, because key2 has followers. ");
                        if (references.ContainsKey(SnapperKeyReferenceType.DeleteReference) && references[SnapperKeyReferenceType.DeleteReference].ContainsKey(info.key2))
                            return (false, $"Fail to PutKeyWithReference, because key2 has followers. ");
                        break;
                    case SnapperKeyReferenceType.ReplicateReference:
                    case SnapperKeyReferenceType.DeleteReference:
                        return (false, "Fail to PutKeyWithReference, because key2 is registered with a reference. ");
                }

                items[info.key2] = newValue;
                updates.Add(new Update(false, UpdateType.Modify, info.key2, items[info.key2], oldValue));
            }
            else
            {
                //if (!accessModePerKey.ContainsKey(info.key2)) accessModePerKey.Add(info.key2, AccessMode.ReadWrite);
                //else Debug.Assert(accessModePerKey[info.key2] == AccessMode.ReadWrite);
                Debug.Assert(accessModePerKey[info.key2] == AccessMode.ReadWrite);

                items.Add(info.key2, newValue);
                updates.Add(new Update(false, UpdateType.Add, info.key2, items[info.key2]));
            }

            return (true, "");
        }
        else
        {
            // in this case, the actor-level concurrency control is used

            // check the allowed access mode on this actor
            Debug.Assert(accessMode == AccessMode.ReadWrite);

            var key2TypeName = info.key2.GetAssemblyQualifiedName();
            Debug.Assert(!string.IsNullOrEmpty(key2TypeName));
            var newValue = new SnapperValue(value, key2TypeName, info.referenceType, info.function, info.grain1, info.key1);

            if (items.ContainsKey(info.key2))
            {
                var existingValue = items[info.key2];
                var oldValue = existingValue.value;
                Debug.Assert(oldValue != null);
                var existingReferenceType = existingValue.referenceType;

                switch (existingReferenceType)
                {
                    case SnapperKeyReferenceType.NoReference:
                        // if key2 is origin of other keys, cannot convert key2 to a follower
                        if (references.ContainsKey(SnapperKeyReferenceType.ReplicateReference) && references[SnapperKeyReferenceType.ReplicateReference].ContainsKey(info.key2))
                            return (false, $"Fail to PutKeyWithReference, because key2 has followers. ");
                        if (references.ContainsKey(SnapperKeyReferenceType.DeleteReference) && references[SnapperKeyReferenceType.DeleteReference].ContainsKey(info.key2))
                            return (false, $"Fail to PutKeyWithReference, because key2 has followers. ");
                        break;
                    case SnapperKeyReferenceType.ReplicateReference:
                    case SnapperKeyReferenceType.DeleteReference:
                        return (false, "Fail to PutKeyWithReference, because key2 is registered with a reference. ");
                }

                items[info.key2] = newValue;
                updates.Add(new Update(false, UpdateType.Modify, info.key2, items[info.key2], oldValue));
            }
            else
            { 
                items.Add(info.key2, newValue);
                updates.Add(new Update(false, UpdateType.Add, info.key2, items[info.key2]));
            }

            return (true, "");
        }
    }

    // ================================================================================ for resolving how to forward updates to followers =====================================
    /// <summary> given the update performed on key1, find all affected keys </summary>
    public Dictionary<GrainID, List<ForwardedUpdate>> GetUpdatesToForward(GrainID myID)
    {
        var updatesPerGrain = new Dictionary<GrainID, List<ForwardedUpdate>>();
        foreach (var update in updates)
        {
            if (update.isForwarded) continue;

            if (update.value.referenceType == SnapperKeyReferenceType.NoReference)
            {
                if (update.updateType == UpdateType.Add) continue;
                else if (update.updateType == UpdateType.Modify)
                {
                    // the updates on the origin key should forward to all followers
                    foreach (var i1 in references)
                    {
                        if (i1.Key != SnapperKeyReferenceType.ReplicateReference && i1.Key != SnapperKeyReferenceType.UpdateReference) continue;
                        if (!i1.Value.ContainsKey(update.key)) continue;

                        foreach (var follower in i1.Value[update.key])
                        {
                            var grain2 = follower.Key;
                            foreach (var iitem in follower.Value)
                            {
                                var key2 = iitem.Key;
                                var function = iitem.Value;  // the function the needs to be applied on the value of key2
                                if (!updatesPerGrain.ContainsKey(grain2)) updatesPerGrain.Add(grain2, new List<ForwardedUpdate>());
                                updatesPerGrain[grain2].Add(new ForwardedUpdate(true, update, i1.Key, grain2, key2, function));
                            }
                        }
                    }
                }
                else
                {
                    // the deletion of the origin key will cause the deletion of all registered references, both for update and delete references
                    var referenceToDelete = new List<ReferenceInfo>();
                    foreach (var i1 in references)
                    {
                        if (i1.Key != SnapperKeyReferenceType.DeleteReference && i1.Key != SnapperKeyReferenceType.ReplicateReference && i1.Key != SnapperKeyReferenceType.UpdateReference) continue;
                        if (!i1.Value.ContainsKey(update.key)) continue;

                        foreach (var follower in i1.Value[update.key])
                        {
                            var grain2 = follower.Key;
                            foreach (var iitem in follower.Value)
                            {
                                var key2 = iitem.Key;
                                var function = iitem.Value;  // the function the needs to be applied on the value of key2
                                if (!updatesPerGrain.ContainsKey(grain2)) updatesPerGrain.Add(grain2, new List<ForwardedUpdate>());
                                updatesPerGrain[grain2].Add(new ForwardedUpdate(true, update, i1.Key, grain2, key2, function));
                                referenceToDelete.Add(new ReferenceInfo(i1.Key, myID, update.key, grain2, key2, function));
                            }
                        }
                    }

                    foreach (var info in referenceToDelete)
                    {
                        Debug.Assert(references[info.referenceType][info.key1][info.grain2].ContainsKey(info.key2));
                        references[info.referenceType][info.key1][info.grain2].Remove(info.key2);
                        updatesOnReference.Add((UpdateType.Delete, info));
                    }
                }
            }
            else
            {
                // when a follower key is deleted, then the registered reference info should be deleted on the origin grain
                if (update.updateType != UpdateType.Delete) continue;

                var grain1 = update.value.dependentGrain;
                var key1 = update.value.dependentKey;
                Debug.Assert(grain1 != null && key1 != null);

                if (!updatesPerGrain.ContainsKey(grain1)) updatesPerGrain.Add(grain1, new List<ForwardedUpdate>());
                updatesPerGrain[grain1].Add(new ForwardedUpdate(false, update, update.value.referenceType, grain1, key1, update.value.function));
            }
        }
        return updatesPerGrain;
    }

    public void ApplyForwardedUpdates(GrainID fromGrain, List<ForwardedUpdate> forwardedUpdates)
    {
        foreach (var forwardedUpdate in forwardedUpdates)
        {
            if (!forwardedUpdate.toFollower)
            {
                // this update is forwarded from a follower to origin
                // when the follower is deleted, the origin grain should delete the registered reference
                var update = forwardedUpdate.update;
                Debug.Assert(update.updateType == UpdateType.Delete);
                var followerValue = update.value;
                Debug.Assert(followerValue.referenceType != SnapperKeyReferenceType.NoReference);
                Debug.Assert(followerValue.function != null);
                Debug.Assert(followerValue.dependentGrain != null && followerValue.dependentKey != null);
                Debug.Assert(forwardedUpdate.affectedGrain.Equals(followerValue.dependentGrain) && forwardedUpdate.affectedKey.Equals(followerValue.dependentKey));
                DeRegisterReference(new ReferenceInfo(followerValue.referenceType, followerValue.dependentGrain, followerValue.dependentKey, fromGrain, update.key, followerValue.function));
            }
            else
            {
                // this update is forwarded from an origin to follower
                var update = forwardedUpdate.update;
                Debug.Assert(update.updateType != UpdateType.Add);

                var referenceType = forwardedUpdate.referenceType;
                var affectedKey = forwardedUpdate.affectedKey;

                if (accessMode == AccessMode.NoOp)
                {
                    Debug.Assert(accessModePerKey.ContainsKey(affectedKey));
                    Debug.Assert(accessModePerKey[affectedKey] == AccessMode.ReadWrite);
                }
                else Debug.Assert(accessMode == AccessMode.ReadWrite);

                var keyExists = items.ContainsKey(affectedKey);
                if (keyExists)
                {
                    var existingValue = items[affectedKey];
                    Debug.Assert(existingValue.referenceType == referenceType);
                    Debug.Assert(existingValue.keyTypeName == affectedKey.GetAssemblyQualifiedName());
                    Debug.Assert(existingValue.dependentGrain != null);
                    Debug.Assert(existingValue.dependentGrain.Equals(fromGrain));
                    Debug.Assert(existingValue.dependentKey != null);
                    Debug.Assert(existingValue.dependentKey.Equals(update.key));
                    Debug.Assert(existingValue.function != null);

                    switch (referenceType)
                    {
                        case SnapperKeyReferenceType.ReplicateReference:
                            Debug.Assert(affectedKey.Equals(update.key));

                            if (update.updateType == UpdateType.Modify)
                            {
                                var oldValue = update.oldValue;    // the old value of the original key before doing Modify operation
                                Debug.Assert(oldValue != null);

                                var newValue = existingValue.function.ApplyUpdate(update.key, oldValue, update.value.value, affectedKey, existingValue.value);  
                                items[affectedKey] = new SnapperValue(newValue, existingValue.keyTypeName, referenceType, existingValue.function, fromGrain, update.key);
                                updates.Add(new Update(true, UpdateType.Modify, affectedKey, items[affectedKey], oldValue));
                            }
                            else    // if the original key is deleted, the follower key should also be deleted
                            {
                                updates.Add(new Update(true, UpdateType.Delete, affectedKey, items[affectedKey]));
                                items.Remove(affectedKey);
                            }
                            break;
                        case SnapperKeyReferenceType.DeleteReference:
                            updates.Add(new Update(true, UpdateType.Delete, affectedKey, items[affectedKey]));
                            items.Remove(affectedKey);
                            break;
                    }
                }
            }
        }
    }
}
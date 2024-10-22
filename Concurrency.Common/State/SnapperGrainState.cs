using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.State;

using ReferencesStructure = Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>;

[GenerateSerializer]
public class SnapperGrainState : IEquatable<SnapperGrainState>, ICloneable
{
    [Id(0)]
    readonly GrainID myID;

    [Id(1)]
    public Dictionary<ISnapperKey, SnapperValue> dictionary;

    [Id(2)]
    public ReferencesStructure references;

    [Id(3)]
    public List<SnapperRecord> list;

    public SnapperGrainState(GrainID myID, Dictionary<ISnapperKey, SnapperValue> dictionary, ReferencesStructure references, List<SnapperRecord> list)
    {
        this.myID = myID;
        this.dictionary = dictionary;
        this.references = references;
        this.list = list;
    }

    public SnapperGrainState(GrainID myID)
    {
        this.myID = myID;
        dictionary = new Dictionary<ISnapperKey, SnapperValue>();
        references = new ReferencesStructure();
        list = new List<SnapperRecord>();
    }

    public void ApplyUpdates(SnapperGrainStateUpdates updates) => ApplyUpdates(updates.updatesOnDictionary, updates.updatesOnReference, updates.updatesOnList);

    public void ApplyUpdates(List<Update> updatesOnDictionary, List<(UpdateType, ReferenceInfo)> updatesOnReference, List<SnapperRecord> updatesOnList)
    {
        // apply updates to the dictionary
        foreach (var update in updatesOnDictionary)
        {
            switch (update.updateType)
            {
                case UpdateType.Add:
                    if (dictionary.ContainsKey(update.key)) throw new SnapperException(ExceptionType.RWConflict);
                    dictionary.Add(update.key, update.value);
                    break;
                case UpdateType.Delete:
                    if (!dictionary.ContainsKey(update.key)) throw new SnapperException(ExceptionType.RWConflict);
                    dictionary.Remove(update.key);
                    break;
                case UpdateType.Modify:
                    if (!dictionary.ContainsKey(update.key)) throw new SnapperException(ExceptionType.RWConflict);
                    dictionary[update.key] = update.value;
                    break;
            }
        }

        // apply updates to reference info
        foreach (var update in updatesOnReference)
        {
            var info = update.Item2;

            switch (update.Item1)
            {
                case UpdateType.Add:
                    if (!references.ContainsKey(info.referenceType)) references.Add(info.referenceType, new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>());
                    if (!references[info.referenceType].ContainsKey(info.key1)) references[info.referenceType].Add(info.key1, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());
                    if (!references[info.referenceType][info.key1].ContainsKey(info.grain2)) references[info.referenceType][info.key1].Add(info.grain2, new Dictionary<ISnapperKey, IUpdateFunction>());
                    references[info.referenceType][info.key1][info.grain2].Add(info.key2, info.function);
                    break;
                case UpdateType.Delete:
                    if (!references.ContainsKey(info.referenceType) ||
                        !references[info.referenceType].ContainsKey(info.key1) ||
                        !references[info.referenceType][info.key1].ContainsKey(info.grain2) ||
                        !references[info.referenceType][info.key1][info.grain2].ContainsKey(info.key2))
                        throw new SnapperException(ExceptionType.RWConflict);
                    references[info.referenceType][info.key1][info.grain2].Remove(info.key2);
                    break;
                default:
                    Debug.Assert(false);
                    break;
            }
        }

        // apply updates to the log list
        foreach (var newLog in updatesOnList) list.Add(newLog);
    }

    public bool Equals(SnapperGrainState? other)
    {
        if (other == null) return false;

        if (!myID.Equals(other.myID)) return false;

        // check if dictionary is the same
        if (!Helper.CheckDictionaryEqualty(dictionary, other.dictionary)) return false;
        
        // check if references are the same
        if (other.references.Count != references.Count) return false;
        
        foreach (var i1 in other.references)
        {
            if (!references.ContainsKey(i1.Key)) return false;
            if (i1.Value.Count != references[i1.Key].Count) return false;

            foreach (var i2 in i1.Value)
            {
                if (!references[i1.Key].ContainsKey(i2.Key)) return false;
                if (i2.Value.Count != references[i1.Key][i2.Key].Count) return false;

                foreach (var i3 in i2.Value)
                {
                    if (!references[i1.Key][i2.Key].ContainsKey(i3.Key)) return false;
                    if (i3.Value.Count != references[i1.Key][i2.Key][i3.Key].Count) return false;

                    if (!Helper.CheckDictionaryEqualty(i3.Value, references[i1.Key][i2.Key][i3.Key])) return false;
                }
            }
        }

        // check if the list is the same
        if (other.list.Count != list.Count) return false;
        
        for (var i = 0; i < other.list.Count; i++) if (!other.list[i].Equals(list[i])) return false;
        
        return true;
    }

    public object Clone()
    {
        var copiedDictionary = Helper.CloneDictionary(dictionary);
        var copiedReferences = CloneReferences(references);
        var copiedList = CloneListState(list);
        return new SnapperGrainState((GrainID)myID.Clone(), copiedDictionary, copiedReferences, copiedList);
    }

    /// <summary> clone the information of acquired keys </summary>
    /// <returns> the cloned dictionary, the cloned references </returns>
    public (Dictionary<ISnapperKey, SnapperValue>, ReferencesStructure) CloneInfoOfKeys(HashSet<ISnapperKey> keys)
    {
        var dictionaryOfKeys = new Dictionary<ISnapperKey, SnapperValue>();
        var referenceOfKeys = new ReferencesStructure();
        foreach (var key in keys)
        {
            // this key might have been deleted by the previous transaction
            if (!dictionary.ContainsKey(key)) continue;

            var value = dictionary[key].Clone() as SnapperValue;
            Debug.Assert(value != null);
            dictionaryOfKeys.Add(key, value);

            // find any reference info that is related to the key
            foreach (var i1 in references)
            {
                var referenceType = i1.Key;
                if (!i1.Value.ContainsKey(key)) continue;
                if (i1.Value[key].Count == 0) continue;

                referenceOfKeys.Add(referenceType, new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>());
                referenceOfKeys[referenceType].Add(key, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());

                foreach (var i3 in i1.Value[key])
                {
                    var grainID = i3.Key.Clone() as GrainID;
                    Debug.Assert(grainID != null);
                    referenceOfKeys[referenceType][key].Add(grainID, Helper.CloneDictionary(i3.Value));
                }
            }
        }

        return (dictionaryOfKeys, referenceOfKeys);
    }

    public static ReferencesStructure CloneReferences(ReferencesStructure references)
    {
        var copiedReferences = new ReferencesStructure();
        foreach (var i1 in references)
        {
            copiedReferences.Add(i1.Key, new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>());
            foreach (var i2 in i1.Value)
            {
                var key = i2.Key.Clone() as ISnapperKey;
                Debug.Assert(key != null);

                copiedReferences[i1.Key].Add(key, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());
                foreach (var i3 in i2.Value)
                {
                    var grainID = i3.Key.Clone() as GrainID;
                    Debug.Assert(grainID != null);

                    copiedReferences[i1.Key][key].Add(grainID, Helper.CloneDictionary(i3.Value));
                }
            }
        }

        return copiedReferences;
    }

    public static List<SnapperRecord> CloneListState(List<SnapperRecord> list)
    {
        var copiedList = new List<SnapperRecord>();
        foreach (var item in list)
        {
            var record = item.Clone() as SnapperRecord;
            Debug.Assert(record != null);
            copiedList.Add(record);
        }

        return copiedList;
    }

    // ========================================================================== for grain migration

    public void SetInitialState(
        Dictionary<ISnapperKey, SnapperValue> dictionary,
        ReferencesStructure references,
        List<SnapperRecord> list)
    {
        this.dictionary = dictionary;
        this.references = references;
        this.list = list;
    }

    public (Dictionary<ISnapperKey, SnapperValue>, ReferencesStructure, List<SnapperRecord>) GetState() => (dictionary, references, list);
}
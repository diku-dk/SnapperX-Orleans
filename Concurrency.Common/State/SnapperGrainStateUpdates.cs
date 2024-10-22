namespace Concurrency.Common.State;

public class SnapperGrainStateUpdates
{
    public List<Update> updatesOnDictionary;

    public List<(UpdateType, ReferenceInfo)> updatesOnReference;

    public List<SnapperRecord> updatesOnList;

    public SnapperGrainStateUpdates(List<Update> updatesOnDictionary, List<(UpdateType, ReferenceInfo)> updatesOnReference, List<SnapperRecord> updatesOnList)
    {
        this.updatesOnDictionary = updatesOnDictionary;
        this.updatesOnReference = updatesOnReference;
        this.updatesOnList = updatesOnList;
    }

    public SnapperGrainStateUpdates()
    {
        updatesOnDictionary = new List<Update>();
        updatesOnReference = new List<(UpdateType, ReferenceInfo)>();
        updatesOnList = new List<SnapperRecord>();
    }

    public void MergeUpdates(SnapperGrainStateUpdates updates) => MergeUpdates(updates.updatesOnDictionary, updates.updatesOnReference, updates.updatesOnList);

    public void MergeUpdates(List<Update> updatesOnDictionary, List<(UpdateType, ReferenceInfo)> updatesOnReference, List<SnapperRecord> updatesOnList)
    {
        foreach (var update in updatesOnDictionary) this.updatesOnDictionary.Add(update);
        foreach (var update in updatesOnReference) this.updatesOnReference.Add(update);
        foreach (var update in updatesOnList) this.updatesOnList.Add(update);
    }
}
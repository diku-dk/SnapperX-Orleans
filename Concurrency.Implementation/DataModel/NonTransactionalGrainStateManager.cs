using Concurrency.Common;
using Concurrency.Common.State;
using Utilities;

namespace Concurrency.Implementation.DataModel;

internal class NonTransactionalGrainStateManager
{
    readonly SnapperGrainID myGrainID;

    SnapperGrainState state;

    DictionaryState dictionaryState;
    ListState listState;

    public NonTransactionalGrainStateManager(SnapperGrainID myGrainID)
    {
        this.myGrainID = myGrainID;
        state = new SnapperGrainState(myGrainID.grainID);
        dictionaryState = new DictionaryState(myGrainID.grainID, ImplementationType.NONTXNKV, AccessMode.ReadWrite, state.dictionary, state.references);
        listState = new ListState(AccessMode.ReadWrite, state.list);
    }

    public (DictionaryState, ListState) GetState() => (dictionaryState, listState);

    public bool IfStateChanged()
    {
        (var updatesOnDictionary, var updatesOnReference) = dictionaryState.GetUpdates();
        var updatesOnList = listState.GetUpdates();

        return updatesOnDictionary.Count != 0 || updatesOnReference.Count != 0 || updatesOnList.Count != 0;
    }
}
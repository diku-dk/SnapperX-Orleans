using Concurrency.Common;
using Concurrency.Common.Logging;
using Concurrency.Common.State;
using Utilities;

namespace Replication.Interface;

using Schedule = Dictionary<SnapperGrainID, Batch>;

public interface IHistoryManager
{
    bool CheckGC();

    void Init(IGrainFactory grainFactory, string myRegionID, string mySiloID, string myRegionalSiloID, Hierarchy myHierarchy);

    // ==================================================================================================================== for replication
    Task RegisterGrainState(SnapperGrainID grainID, PrepareLog log, SnapperGrainStateUpdates updates);

    /// <summary> this is only called by the regional coordinator </summary>
    void RegisterAccessInfo(SnapperID id, int numSilos);

    /// <summary> this is only called by the local coordinator </summary>
    void RegisterAccessInfo(SnapperID id, bool multiSilo, HashSet<SnapperGrainID> grains);

    Task ACKCompletion(SnapperID id);

    // =============================================================================================== for read-only transactions on replica
    
    /// <summary> use the PACTs received so far to form a batch </summary>
    Task<Schedule> GenerateBatch(List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList);

    /// <summary> convert received higher level batches in specified order </summary>
    Task<List<Schedule>> ProcessBatches(SortedDictionary<SnapperID, Batch> batches, Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>> higherPACTList);

    Task<SnapperGrainState> WaitForTurn(SnapperGrainID grainID, TransactionContext cxt);

    Task CompleteTxn(SnapperGrainID grainID, TransactionContext cxt);

    Task<SnapperID> NewACT();
}
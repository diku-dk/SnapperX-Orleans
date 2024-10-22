using Concurrency.Common;
using Concurrency.Interface.Coordinator;
using Utilities;

namespace Concurrency.Interface;

using Schedule = Dictionary<SnapperGrainID, Batch>;

public interface IScheduleManager
{
    AggregatedExperimentData CheckGC();

    void UpdateCommitInfo(CommitInfo commitInfo);

    void Init(IGrainFactory grainFactory, string myRegionID, string mySiloID, Hierarchy myHierarchy);

    (Dictionary<SnapperGrainID, IGlobalCoordGrain>, Dictionary<SnapperGrainID, IRegionalCoordGrain>, Dictionary<SnapperGrainID, ILocalCoordGrain>) GetAllCoordInfo();

    public SnapperID NewACT();

    (SnapperID, SnapperID, SnapperGrainID, Schedule) GenerateBatch(SnapperGrainID coordID, List<(TaskCompletionSource<(SnapperID, SnapperID, List<SnapperGrainID>)>, ActorAccessInfo)> pactList);

    List<(SnapperID, SnapperID, SnapperGrainID, Schedule)> ProcessBatches(SnapperGrainID coordID, SortedDictionary<SnapperID, Batch> batches, Dictionary<SnapperID, TaskCompletionSource<List<SnapperGrainID>>> higherPACTList);
}
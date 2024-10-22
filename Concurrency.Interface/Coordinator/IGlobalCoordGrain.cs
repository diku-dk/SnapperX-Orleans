using Concurrency.Common;
using Orleans.Concurrency;

namespace Concurrency.Interface.Coordinator;

/// <summary> guid + region ID </summary>
public interface IGlobalCoordGrain : IGrainWithGuidCompoundKey
{
    Task Init();

    Task<AggregatedExperimentData> CheckGC();

    [OneWay]
    Task PassToken();

    /// <summary> create a new multi-home PACT </summary>
    Task<(SnapperID, SnapperID, List<SnapperGrainID>, CommitInfo)> NewPACT(Immutable<ActorAccessInfo> actorAccessInfo);

    [OneWay]
    Task ACKBatchComplete(SnapperID primaryBid, HashSet<GrainID> writers);

    Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid);

    Task<(SnapperID, CommitInfo)> NewACT();
}
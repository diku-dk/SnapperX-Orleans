using Orleans.Concurrency;
using Concurrency.Common;

namespace Concurrency.Interface.Coordinator;

/// <summary> guid + region ID </summary>
public interface IRegionalCoordGrain : IGrainWithGuidCompoundKey
{
    Task Init();

    Task<AggregatedExperimentData> CheckGC();

    [OneWay]
    Task PassToken();

    /// <summary> create a new single-region multi-silo PACT </summary>
    Task<(SnapperID, SnapperID, List<SnapperGrainID>, CommitInfo)> NewPACT(Immutable<ActorAccessInfo> actorAccessInfo);

    Task<(List<SnapperGrainID>, CommitInfo)> NewPACT(SnapperID primaryBid, SnapperID primaryTid, Immutable<ActorAccessInfo> actorAccessInfo);

    /// <summary> this batch is sent from global coordinators </summary>
    [OneWay]
    Task ReceiveBatch(Batch batch, CommitInfo highCommitInfo);

    [OneWay]
    Task ACKBatchComplete(SnapperID primaryBid, HashSet<GrainID> writers);

    [OneWay]
    Task CommitHighBatch(SnapperID primaryBid, CommitInfo highCommitInfo);

    Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid);
}
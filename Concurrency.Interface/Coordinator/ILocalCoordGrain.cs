using Orleans.Concurrency;
using Concurrency.Common;

namespace Concurrency.Interface.Coordinator;

/// <summary> guid + region ID + internal region ID </summary>
public interface ILocalCoordGrain : IGrainWithGuidCompoundKey
{
    Task Init();

    Task<AggregatedExperimentData> CheckGC();

    [OneWay]
    Task PassToken();

    /// <summary> create a new single-home single-silo PACT </summary>
    Task<(SnapperID, SnapperID, List<SnapperGrainID>, CommitInfo)> NewPACT(Immutable<ActorAccessInfo> actorAccessInfo);

    Task<CommitInfo> NewPACT(SnapperID primaryBid, SnapperID primaryTid, Immutable<ActorAccessInfo> actorAccessInfo);

    /// <summary> this batch is sent from regional coordinators </summary>
    [OneWay]
    Task ReceiveBatch(Batch batch, CommitInfo highCommitInfo);

    [OneWay]
    Task ACKBatchCompleteOnGrain(SnapperGrainID grainID, SnapperID primaryBid, bool isWriter);

    [OneWay]
    Task CommitHighBatch(SnapperID primaryBid, CommitInfo highCommitInfo);

    Task WaitForBatchCommit(SnapperID primaryBid, SnapperID bid);
}
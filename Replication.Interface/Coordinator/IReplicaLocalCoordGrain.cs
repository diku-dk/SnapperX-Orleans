using Concurrency.Common;

namespace Replication.Interface.Coordinator;

public interface IReplicaLocalCoordGrain: IGrainWithGuidCompoundKey
{
    /// <summary> the set of grains only include the ones in the current local silo </summary>
    Task RegisterAccessInfo(SnapperID id, bool multiSilo, HashSet<SnapperGrainID> grains);

    /// <summary> create a new single-region single-silo PACT </summary>
    Task<(SnapperID, SnapperID)> NewPACT(ActorAccessInfo actorAccessInfo);

    /// <summary> receive regional batch from regional coordiantors </summary>
    Task ReceiveBatch(Batch batch);

    Task CheckGC();
}
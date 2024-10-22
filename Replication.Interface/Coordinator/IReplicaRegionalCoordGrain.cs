using Concurrency.Common;

namespace Replication.Interface.Coordinator;

public interface IReplicaRegionalCoordGrain : IGrainWithGuidCompoundKey
{
    Task<SnapperID> NewACT();

    Task RegisterAccessInfo(SnapperID id, int numServices);

    /// <summary> create a new single-region multi-silo PACT </summary>
    Task<(SnapperID, SnapperID)> NewPACT(ActorAccessInfo actorAccessInfo);

    Task ACKCompletion(SnapperID id);

    Task CheckGC();
}
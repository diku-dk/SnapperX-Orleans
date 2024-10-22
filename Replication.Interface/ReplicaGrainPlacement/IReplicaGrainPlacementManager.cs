using Concurrency.Common;

namespace Replication.Interface.GrainReplicaPlacement;

public interface IReplicaGrainPlacementManager: IGrainWithGuidCompoundKey
{
    Task CheckGC();

    Task Init();

    /// <summary> submit a PACT </summary>
    Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc, List<GrainID> grains);

    /// <summary> submit an ACT </summary>
    Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc);
}
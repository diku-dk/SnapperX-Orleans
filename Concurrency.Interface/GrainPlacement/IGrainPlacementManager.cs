using Concurrency.Common;
using Concurrency.Common.State;

namespace Concurrency.Interface.GrainPlacement;

public interface IGrainPlacementManager : IGrainWithGuidCompoundKey
{
    Task<AggregatedExperimentData> CheckGC();

    /// <summary> submit a PACT </summary>
    Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc, List<GrainID> grains, Dictionary<GrainID, HashSet<ISnapperKey>>? keysToAccessPerGrain = null);

    /// <summary> submit an ACT </summary>
    Task<TransactionResult> SubmitTransaction(GrainID startGrainID, FunctionCall startFunc);

    /// <summary> use KeyValueNonTransactionalGrain, with kv data model, but without concurrency control </summary>
    Task<TransactionResult> SubmitNonTransactionalRequest(GrainID startGrainID, FunctionCall startFunc);

    // ================================================================ for grain migration

    /// <summary> freeze the grain so it does not accept more PACTs, return when all registered PACTs have committed </summary>
    Task FreezeGrain(GrainID grainID);

    Task UnFreezeGrain(GrainID grainID);


    Task<TransactionContext> NewPACT(List<GrainID> grains, Dictionary<GrainID, HashSet<ISnapperKey>>? keysToAccessPerGrain = null);
}
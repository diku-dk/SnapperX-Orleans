using Concurrency.Common;
using Concurrency.Common.State;

namespace Experiment.Common;

public class RequestData
{
    public readonly bool isMultiSiloTxn;
    public readonly bool isMultiRegionTxn;

    public readonly string transactionType;
    public readonly List<GrainID> grains;
    public readonly object? funcInput;

    /// <summary> only for key-level concurrency control </summary>
    public readonly Dictionary<GrainID, HashSet<ISnapperKey>>? accessedKeysPerGrain;

    public RequestData(bool isMultiSiloTxn, bool isMultiRegionTxn, string transactionType, List<GrainID> grains, Dictionary<GrainID, HashSet<ISnapperKey>>? accessedKeysPerGrain = null, object? funcInput = null)
    {
        this.isMultiSiloTxn = isMultiSiloTxn;
        this.isMultiRegionTxn = isMultiRegionTxn;
        
        this.transactionType = transactionType;
        this.grains = grains;
        this.accessedKeysPerGrain = accessedKeysPerGrain;

        this.funcInput = funcInput;
    }
}
using Concurrency.Common;

namespace Concurrency.Interface.DataModel;

public interface INonTransactionalKeyValueGrain : IGrainWithGuidCompoundKey
{
    Task<AggregatedExperimentData> CheckGC();

    Task<FunctionResult> Execute(FunctionCall call, SnapperID tid);
}
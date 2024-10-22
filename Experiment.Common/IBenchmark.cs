using Concurrency.Common;

namespace Experiment.Common;

public interface IBenchmark
{
    Task<TransactionResult> NewTransaction(RequestData data, bool isReplica);
}
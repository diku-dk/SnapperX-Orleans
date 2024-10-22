using Concurrency.Common;
using Concurrency.Common.Logging;

namespace Replication.Interface.TransactionReplication;

public interface ITransactionReplicationGrain : IGrainWithGuidCompoundKey
{
    Task CheckGC();

    Task Init();

    Task<TransactionResult> ExecuteACT(FunctionCall startFunc);

    Task<FunctionResult> Execute(FunctionCall startFunc, TransactionContext cxt);

    Task Commit(TransactionContext cxt);

    // for debugging
    Task RegisterGrainState(PrepareLog log);
}
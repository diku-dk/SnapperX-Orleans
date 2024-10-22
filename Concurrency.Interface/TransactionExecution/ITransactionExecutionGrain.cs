using Concurrency.Common;
using Orleans.Concurrency;

namespace Concurrency.Interface.TransactionExecution;

public interface ITransactionExecutionGrain : IGrainWithGuidCompoundKey
{
    Task<AggregatedExperimentData> CheckGC();

    [OneWay]
    Task ReceiveBatch(Immutable<Batch> batch, CommitInfo commitInfo);

    [OneWay]
    Task CommitBatch(SnapperID primaryBid, CommitInfo commitInfo);

    Task<TransactionResult> ExecutePACT(FunctionCall startFunc, TransactionContext cxt, DateTime registeredTime);

    /// <summary> start the execution of an ACT </summary>
    Task<TransactionResult> ExecuteACT(FunctionCall startFunc);

    Task<FunctionResult> Execute(FunctionCall call, TransactionContext cxt);

    /// <summary> this message is sent from coordinator to parcipant actors when read or write locks have been acquired </summary>
    /// <return> if there is anything to persist, if the grain state has been changed by the ACT </return>
    Task<(bool, bool)> Prepare(SnapperID tid);

    /// <summary> this message is sent from coordinator to parcipant actors when write locks have been acquired </summary>
    Task Commit(SnapperID tid);

    Task Abort(SnapperID tid);

    // ================================================================ for grain migration

    Task ActivateGrain();

    Task ActivateGrain(DateTime prevTimestamp, SnapperID prevPreparedVersion, byte[] dictionary, byte[] updateReference, byte[] deleteReference, byte[] list);

    /// <summary> freeze the grain so it does not accept more ACTs, return when all entered ACTs have committed / aborted </summary>
    Task FreezeGrain();

    /// <summary> take a snapshot of the grain state and de-activate the grain on Orleans runtime </summary>
    Task<(DateTime, SnapperID, byte[], byte[], byte[], byte[])> DeactivateGrain();
}
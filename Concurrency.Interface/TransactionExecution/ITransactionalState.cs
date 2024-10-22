namespace Concurrency.Interface.TransactionExecution;

public interface ITransactionalState<TState>
{
    void CheckGC();

    // PACT + ACT
    void SetState(TState state);

    TState GetCommittedState();

    // ACT
    Task<TState> NonDetRead(long tid);

    Task<TState> NonDetReadWrite(long tid);

    Task<bool> Prepare(long tid, bool isReader);

    void Commit(long tid);

    void Abort(long tid);

    TState GetPreparedState(long tid);
}
using Concurrency.Interface.TransactionExecution;
using Concurrency.Common;

namespace TestApp.Interfaces;

public interface ISnapperTransactionalTestGrain : ITransactionExecutionGrain
{
    Task<object?> Init(TransactionContext cxt, object? obj = null);
    Task<object?> DoOp(TransactionContext cxt, object? obj = null);

    Task<object?> ReadState(TransactionContext cxt, object? obj = null);
}
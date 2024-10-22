using Concurrency.Common;
using Replication.Interface.TransactionReplication;

namespace TestApp.Interfaces;

public interface ISnapperReplicatedTestGrain : ITransactionReplicationGrain
{
    Task<object?> DoOp(TransactionContext cxt, object? obj = null);
}
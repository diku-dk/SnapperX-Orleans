using Concurrency.Common;

namespace SmallBank.Interfaces;

public interface IOrleansTransactionalAccountGrain : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> MultiTransfer(object? obj = null);

    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> Deposit(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReadState(object? obj = null);
}
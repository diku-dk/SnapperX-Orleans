using Concurrency.Common;

namespace SmallBank.Interfaces;

public interface INonTransactionalSimpleAccountGrain : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> MultiTransfer(object? obj = null);

    Task<TransactionResult> Deposit(object? obj = null);

    Task<TransactionResult> ReadState();
}
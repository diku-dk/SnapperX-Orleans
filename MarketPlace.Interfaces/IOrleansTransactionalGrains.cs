using Concurrency.Common;

namespace MarketPlace.Interfaces;

public interface IOrleansTransactionalSellerActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> AggregateOrderInfo(object? obj = null);
}

public interface IOrleansTransactionalCartActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> AddItemToCart(object? obj = null);

    [Transaction(TransactionOption.CreateOrJoin)]
    Task<TransactionResult> DeleteItemInCart(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Checkout(object? obj = null);

    /// <summary> when a product is updated or deleted by the product actor, it should forward to the cart actor </summary>
    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> UpdateOrDeleteProduct(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReadState();
}

public interface IOrleansTransactionalCustomerActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);
}

public interface IOrleansTransactionalOrderActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> NewOrder(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReadState();
}

public interface IOrleansTransactionalPaymentActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> ProcessPayment(object? obj = null);
}

public interface IOrleansTransactionalProductActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> UpdatePrice(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> DeleteProduct(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> AddProducts(object? obj = null);

    /// <summary> this method is called by cart or stock actors </summary>
    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> RemoveDependency(object? obj = null);

    /// <summary> this method is called by cart or stock actors </summary>
    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> AddDependency(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReadState();
}

public interface IOrleansTransactionalShipmentActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> NewPackage(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReadState();
}

public interface IOrleansTransactionalStockActor : IGrainWithGuidCompoundKey
{
    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> Init(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> AddStock(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReplenishStock(object? obj = null);

    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> ReduceStock(object? obj = null);

    /// <summary> when a product is deleted on the product actor, it should also be deleted on the stock actor </summary>
    [Transaction(TransactionOption.Join)]
    Task<TransactionResult> DeleteProduct(object? obj = null);

    [Transaction(TransactionOption.Create)]
    Task<TransactionResult> ReadState();
}
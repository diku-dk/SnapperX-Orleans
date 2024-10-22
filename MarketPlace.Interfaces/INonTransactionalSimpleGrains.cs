using Concurrency.Common;

namespace MarketPlace.Interfaces;

public interface INonTransactionalSimpleSellerActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> AggregateOrderInfo(object? obj = null);
}

public interface INonTransactionalSimpleCartActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> AddItemToCart(object? obj = null);

    Task<TransactionResult> DeleteItemInCart(object? obj = null);

    Task<TransactionResult> Checkout(object? obj = null);

    Task<TransactionResult> UpdateOrDeleteProduct(object? obj = null);

    Task<TransactionResult> ReadState();
}

public interface INonTransactionalSimpleCustomerActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);
}

public interface INonTransactionalSimpleOrderActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> NewOrder(object? obj = null);

    Task<TransactionResult> ReadState();
}

public interface INonTransactionalSimplePaymentActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> ProcessPayment(object? obj = null);
}

public interface INonTransactionalSimpleProductActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> UpdatePrice(object? obj = null);

    Task<TransactionResult> DeleteProduct(object? obj = null);

    Task<TransactionResult> AddProducts(object? obj = null);

    Task<TransactionResult> RemoveDependency(object? obj = null);

    Task<TransactionResult> AddDependency(object? obj = null);

    Task<TransactionResult> ReadState();
}

public interface INonTransactionalSimpleShipmentActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> NewPackage(object? obj = null);

    Task<TransactionResult> ReadState();
}

public interface INonTransactionalSimpleStockActor : IGrainWithGuidCompoundKey
{
    Task<TransactionResult> Init(object? obj = null);

    Task<TransactionResult> AddStock(object? obj = null);

    Task<TransactionResult> ReplenishStock(object? obj = null);

    Task<TransactionResult> ReduceStock(object? obj = null);

    Task<TransactionResult> DeleteProduct(object? obj = null);

    Task<TransactionResult> ReadState();
}
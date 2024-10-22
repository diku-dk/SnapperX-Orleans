using Concurrency.Interface.TransactionExecution;

namespace MarketPlace.Interfaces.ISnapperTransactionalGrains;

public interface ICustomerActor : ITransactionExecutionGrain { }

public interface ICartActor : ITransactionExecutionGrain { }

public interface IProductActor : ITransactionExecutionGrain { }

public interface IOrderActor : ITransactionExecutionGrain { }

public interface IStockActor : ITransactionExecutionGrain { }

public interface IPaymentActor : ITransactionExecutionGrain { }

public interface IShipmentActor : ITransactionExecutionGrain { }

public interface ISellerActor : ITransactionExecutionGrain { }
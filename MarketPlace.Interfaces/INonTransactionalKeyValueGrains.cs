using Concurrency.Interface.DataModel;

namespace MarketPlace.Interfaces;

public interface INonTransactionalKeyValueSellerActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValueCustomerActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValueCartActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValueProductActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValueOrderActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValueStockActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValuePaymentActor : INonTransactionalKeyValueGrain { }

public interface INonTransactionalKeyValueShipmentActor : INonTransactionalKeyValueGrain { }
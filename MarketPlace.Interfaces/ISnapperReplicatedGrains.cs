using Replication.Interface.TransactionReplication;

namespace MarketPlace.Interfaces;

public interface IReplicaSellerActor : ITransactionReplicationGrain { }

public interface IReplicaStockActor : ITransactionReplicationGrain { }

public interface IReplicaShipmentActor : ITransactionReplicationGrain { }
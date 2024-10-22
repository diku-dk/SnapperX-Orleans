using Concurrency.Common.ICache;
using MarketPlace.Interfaces;
using Replication.Implementation.TransactionReplication;
using Replication.Interface;

namespace MarketPlace.Grains.SnapperReplicatedKeyValueGrains;

public class SnapperReplicatedKeyValueShipmentActor : TransactionReplicationGrain, IReplicaShipmentActor
{
    int sellerID;

    int cityID;

    public SnapperReplicatedKeyValueShipmentActor(IHistoryManager historyManager, ISnapperReplicaCache snapperReplicaCache, ISnapperClusterCache snapperClusterCache)
        : base(typeof(SnapperReplicatedKeyValueShipmentActor).FullName, historyManager, snapperReplicaCache, snapperClusterCache) { }
}
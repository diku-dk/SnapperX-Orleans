using Concurrency.Common.Cache;

namespace Concurrency.Common.ICache;

public interface ISnapperReplicaCache : ISnapperCache
{
    void PrepareCache(StaticReplicaInfo staticReplicaInfo, Dictionary<GrainID, int> grainIDToPartitionID, Dictionary<int, string> partitionIDToSiloID);

    (bool, string) GetReplicaGrainLocation(GrainID grainID);

    List<Guid> GetPMListPerSilo(string regionID, string siloID);

    List<int> GetReplicaPartitionsInSilo(string siloID);

    Dictionary<int, string> GetReplicaPartitionsInRegion();

    void PrintReplicaGrainInfo();
}
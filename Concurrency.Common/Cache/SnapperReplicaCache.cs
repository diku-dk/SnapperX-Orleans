using Concurrency.Common.ICache;

namespace Concurrency.Common.Cache;

public class SnapperReplicaCache : ISnapperReplicaCache
{
    StaticReplicaInfo info;

    readonly ISnapperClusterCache snapperClusterCache;

    // only include replicated grains that are located in the current region
    Dictionary<GrainID, int> grainIDToPartitionID;  // grain ID => partition ID, include all master grain info
    Dictionary<int, string> partitionIDToSiloID;    // partition ID => silo ID, include all replicated partitions in the current region

    public SnapperReplicaCache(ISnapperClusterCache snapperClusterCache)
    {
        this.snapperClusterCache = snapperClusterCache;
        grainIDToPartitionID = new Dictionary<GrainID, int>();
        partitionIDToSiloID = new Dictionary<int, string>();
    }

    public void PrepareCache(StaticReplicaInfo staticReplicaInfo, Dictionary<GrainID, int> grainIDToPartitionID, Dictionary<int, string> partitionIDToSiloID)
    {
        this.info = staticReplicaInfo;
        this.grainIDToPartitionID = grainIDToPartitionID;
        this.partitionIDToSiloID = partitionIDToSiloID;
        Console.WriteLine($"SnapperReplicaCache: init {grainIDToPartitionID.Count} replica grain placement info. ");
    }

    /// <returns> if this region has a replica of this grain, the silo ID where the grain replica is located in this region </returns>
    public (bool, string) GetReplicaGrainLocation(GrainID grainID)
    {
        if (!grainIDToPartitionID.ContainsKey(grainID)) return (false, "");
        if (partitionIDToSiloID.ContainsKey(grainIDToPartitionID[grainID])) return (true, partitionIDToSiloID[grainIDToPartitionID[grainID]]);
        else return (false, "");
    }

    public Guid GetOneRandomRegionalCoord(string regionID) => info.regionalCoordList[regionID][new Random().Next(0, info.regionalCoordList[regionID].Count)];

    public Guid GetOneRandomLocalCoord(string regionID, string siloID) => info.localCoordList[regionID][siloID][new Random().Next(0, info.localCoordList[regionID][siloID].Count)];

    public List<Guid> GetAllRegionalCoords(string regionID) => info.regionalCoordList[regionID];

    public List<Guid> GetAllLocalCoords(string regionID, string siloID) => info.localCoordList[regionID][siloID];

    public Dictionary<string, List<Guid>> GetPMList(string regionID) => info.pmList[regionID];

    public List<Guid> GetPMListPerSilo(string regionID, string siloID) => info.pmList[regionID][siloID];

    public List<int> GetReplicaPartitionsInSilo(string siloID)
    {
        var partitions = new List<int>();
        foreach (var item in partitionIDToSiloID) if (item.Value.Equals(siloID)) partitions.Add(item.Key);
        return partitions;
    }

    public Dictionary<int, string> GetReplicaPartitionsInRegion() => partitionIDToSiloID;

    public List<string> GetLocalSiloList(string regionID) => info.localReplicaSiloListPerRegion[regionID];

    public Guid GetOneRandomPlacementManager(string regionID, string siloID)
    {
        var pmList = info.pmList[regionID][siloID];
        return pmList[new Random().Next(0, pmList.Count)];
    }

    public string GetRegionalSiloID(string regionID) => snapperClusterCache.GetRegionalSiloID(regionID);

    public void PrintReplicaGrainInfo()
    {
        foreach (var item in grainIDToPartitionID)
            Console.WriteLine($"grainIDToPartitionID: grainID = {item.Key.Print()}, partitionID = {item.Value}");

        foreach (var item in partitionIDToSiloID)
            Console.WriteLine($"partitionIDToSiloID: partitionID = {item.Key}, silo = {item.Value}");
    }
}
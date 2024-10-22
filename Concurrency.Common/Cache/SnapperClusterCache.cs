using Utilities;
using Concurrency.Common.ICache;
using System.Diagnostics;

namespace Concurrency.Common.Cache;

public class SnapperClusterCache : ISnapperClusterCache
{
    StaticClusterInfo info;                                        // this is loaded from Redis

    Dictionary<GrainID, int> grainIDToPartitionID;                 // grain ID => partition ID
    Dictionary<int, (string, string)> partitionIDToMasterInfo;     // partition ID => (master region ID, master silo ID)

    Dictionary<string, List<IClusterClient>> clients;              // region ID, list of clients

    Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker;  // grain ID => migration worker (guid, location)

    Random rnd;

    public SnapperClusterCache()
    {
        info = new StaticClusterInfo();
        grainIDToPartitionID = new Dictionary<GrainID, int>();
        partitionIDToMasterInfo = new Dictionary<int, (string, string)>();
        clients = new Dictionary<string, List<IClusterClient>>();
        grainIDToMigrationWorker = new Dictionary<GrainID, (Guid, string)>();
        rnd = new Random();
    }

    public void PrepareCache(
        StaticClusterInfo staticClusterInfo,
        Dictionary<GrainID, int> grainIDToPartitionID, 
        Dictionary<int, (string, string)> partitionIDToMasterInfo,
        Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker)
    {
        this.info = staticClusterInfo;
        this.grainIDToPartitionID = grainIDToPartitionID;
        this.partitionIDToMasterInfo = partitionIDToMasterInfo;
        this.grainIDToMigrationWorker = grainIDToMigrationWorker;
        Console.WriteLine($"SnapperClusterCache: init {grainIDToPartitionID.Count} grain placement info. ");
    }

    public void SetOrlenasClients(Dictionary<string, List<IClusterClient>> clients) => this.clients = clients;

    public IClusterClient GetOrleansClient(string regionID) => clients[regionID][rnd.Next(0, clients[regionID].Count)];

    public (List<(string, string, Guid)>, List<(string, string, Guid)>, List<(string, string, Guid)>) GetAllCoordInfo()
    { 
        var global = new List<(string, string, Guid)>();
        foreach (var item in info.globalCoordList) global.Add((info.globalRegion, info.globalSiloAddress, item));
        
        var regional = new List<(string, string, Guid)>();
        foreach (var item in info.regionalCoordList)
        {
            var regionID = item.Key;
            item.Value.ForEach(x => regional.Add((regionID, info.regionalSiloPerRegion[regionID], x)));
        }

        var local = new List<(string, string, Guid)>();
        foreach (var item in info.localCoordList)
        {
            var regionID = item.Key;
            foreach (var iitem in item.Value)
            {
                var siloID = iitem.Key;
                iitem.Value.ForEach(x => local.Add((regionID, siloID, x)));
            }
        }

        return (global, regional, local);
    }
      
    /// <summary> find the neighbor coordinator to pass token to </summary>
    public (bool, Guid) GetNeighbor(Hierarchy hierarchy, string regionID, string siloID, Guid coordID)
    {
        List<Guid> list;
        switch (hierarchy)
        {
            case Hierarchy.Global:
                list = info.globalCoordList;
                break;
            case Hierarchy.Regional:
                list = info.regionalCoordList[regionID];
                break;
            case Hierarchy.Local:
                list = info.localCoordList[regionID][siloID];
                break;
            default:
                throw new Exception($"The hierarchy {hierarchy} does not exist. ");
        }

        var index = 0;
        while (list[index] != coordID) index++;

        var isFirst = index == 0;
        if (index == list.Count() - 1) return (isFirst, list[0]);
        else return (isFirst, list[index + 1]);
    }

    public Guid GetOneRandomGlobalCoord() => info.globalCoordList[rnd.Next(0, info.globalCoordList.Count)];

    public Guid GetOneRandomRegionalCoord(string regionID) => info.regionalCoordList[regionID][rnd.Next(0, info.regionalCoordList[regionID].Count)];

    public Guid GetOneRandomLocalCoord(string regionID, string siloID) => info.localCoordList[regionID][siloID][rnd.Next(0, info.localCoordList[regionID][siloID].Count)];

    public (bool, bool) IsSpeculative() => (info.speculativeACT, info.speculativeBatch);

    public double GetBatchSize(Hierarchy hierarchy)
    {
        switch (hierarchy)
        {
            case Hierarchy.Global: return info.globalBatchSizeInMSecs;
            case Hierarchy.Regional: return info.regionalBatchSizeInMSecs;
            case Hierarchy.Local: return info.localBatchSizeInMSecs;
            default: throw new Exception($"Unsupported hierarchy {hierarchy}");
        }
    }

    public string GetRegionalSiloID(string regionID) => info.regionalSiloPerRegion[regionID];

    public string GetGlobalRegion() => info.globalRegion;

    public List<string> GetRegionList() => info.regionList;

    public Guid GetOneRandomPlacementManager(string regionID, string siloID) => info.pmList[regionID][siloID][rnd.Next(0, info.pmList[regionID][siloID].Count)];

    public Dictionary<string, List<Guid>> GetPMList(string regionID) => info.pmList[regionID];

    public List<string> GetLocalSiloList(string regionID) => info.localSiloListPerRegion[regionID];

    public (string, string) GetGlobalRegionAndSilo() => (info.globalRegion, info.globalSiloAddress);

    public SnapperGrainID GetMasterGrain(GrainID grainID)
    {
        var partitionID = grainIDToPartitionID[grainID];
        (var regionID, var siloID) = partitionIDToMasterInfo[partitionID];
        return new SnapperGrainID(grainID.id, regionID + "+" + siloID, grainID.className);
    }

    public List<Guid> GetAllGlobalCoords() => info.globalCoordList;

    public List<Guid> GetAllRegionalCoords(string regionID) => info.regionalCoordList[regionID];

    public List<Guid> GetAllLocalCoords(string regionID, string siloID) => info.localCoordList[regionID][siloID];

    public int GetPartitionID(GrainID grainID) => grainIDToPartitionID[grainID];

    public List<int> getAllMasterPartitionIDs() => partitionIDToMasterInfo.Keys.ToList();

    // ============================================================================================================= for grain migration

    public (Guid, string) GetGrainMigrationWorker(GrainID grainID) => grainIDToMigrationWorker[grainID];

    /// <returns> grain guid, location (region ID "+" silo ID) </returns>
    public List<(Guid, string)> GetAllGrainPlacementManagers()
    {
        var pm = new List<(Guid, string)>();
        foreach (var item in info.pmList)
        {
            var regionID = item.Key;
            foreach (var iitem in item.Value)
            {
                var siloID = iitem.Key;
                pm.AddRange(iitem.Value.Select(guid => (guid, regionID + "+" + siloID)));
            }
        }
        return pm;
    }

    /// <returns> one migration worker per silo (including global, regioanl and local silos) </returns>
    public List<(Guid, string)> GetOneRandomMigrationWorkerPerSilo()
    {
        var mw = new List<(Guid, string)>();
        foreach (var item in info.mwList)
        {
            var regionID = item.Key;
            foreach (var iitem in item.Value)
            {
                var siloID = iitem.Key;
                var selectedGuid = iitem.Value[rnd.Next(0, iitem.Value.Count)];
                mw.Add((selectedGuid, regionID + "+" + siloID));
            }
        }
        return mw;
    }

    public void UpdateUserGrainInfoInCache(GrainID grainID, int targetPartitionID)
    {
        Debug.Assert(grainIDToPartitionID[grainID] != targetPartitionID);
        grainIDToPartitionID[grainID] = targetPartitionID;
    }

    public string GetLocation(int partitionID)
    {
        (var regionID, var siloID) = partitionIDToMasterInfo[partitionID];
        return regionID + "+" + siloID;
    }
}
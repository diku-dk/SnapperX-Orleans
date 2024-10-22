using Concurrency.Common.Cache;
using Utilities;

namespace Concurrency.Common.ICache;

public interface ISnapperClusterCache : ISnapperCache
{
    void PrepareCache(StaticClusterInfo staticClusterInfo, Dictionary<GrainID, int> grainIDToPartitionID, Dictionary<int, (string, string)> partitionIDToMasterInfo, Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker);

    void SetOrlenasClients(Dictionary<string, List<IClusterClient>> clients);

    IClusterClient GetOrleansClient(string regionID);

    (List<(string, string, Guid)>, List<(string, string, Guid)>, List<(string, string, Guid)>) GetAllCoordInfo();

    Guid GetOneRandomGlobalCoord();

    (bool, Guid) GetNeighbor(Hierarchy hierarchy, string regionID, string siloID, Guid coordDI);

    (bool, bool) IsSpeculative();

    double GetBatchSize(Hierarchy hierarchy);

    (string, string) GetGlobalRegionAndSilo();

    string GetGlobalRegion();

    List<string> GetRegionList();

    SnapperGrainID GetMasterGrain(GrainID grainID);

    List<Guid> GetAllGlobalCoords();

    int GetPartitionID(GrainID grainID);

    List<int> getAllMasterPartitionIDs();

    // ========================================================================================== for grain migration

    /// <summary> each grain has a fixed migration worker </summary>
    (Guid, string) GetGrainMigrationWorker(GrainID grainID);

    /// <summary> return the complete list of PMs in the whole cluster </summary>
    List<(Guid, string)> GetAllGrainPlacementManagers();

    List<(Guid, string)> GetOneRandomMigrationWorkerPerSilo();

    void UpdateUserGrainInfoInCache(GrainID grainID, int targetPartitionID);

    /// <summary> get the location of the partition </summary>
    string GetLocation(int partitionID);
}
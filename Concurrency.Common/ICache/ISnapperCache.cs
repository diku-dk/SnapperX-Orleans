namespace Concurrency.Common.ICache;

public interface ISnapperCache
{
    Guid GetOneRandomRegionalCoord(string regionID);

    Guid GetOneRandomLocalCoord(string regionID, string siloID);

    Dictionary<string, List<Guid>> GetPMList(string regionID);

    List<string> GetLocalSiloList(string regionID);

    List<Guid> GetAllRegionalCoords(string regionID);

    List<Guid> GetAllLocalCoords(string regionID, string siloID);

    Guid GetOneRandomPlacementManager(string regionID, string siloID);

    string GetRegionalSiloID(string regionID);
}
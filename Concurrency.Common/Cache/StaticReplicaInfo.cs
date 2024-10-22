using MessagePack;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.Cache;

/// <summary> 
/// each region contains one snapper cluster, each is considered a replica
/// within a region, it may include some master partitions and some replica partitions
/// master partition: all grain in this partition are master copies
/// replica partition: all grains in this partition are replicas
/// </summary>
[MessagePackObject]
public class StaticReplicaInfo
{
    /// <summary> the list of regions (do not include the one that hosts the global silo) </summary>
    [Key(0)]
    public List<string> regionList;

    /// <summary> region ID, the list of local replica silo IDs </summary>
    [Key(1)]
    public Dictionary<string, List<string>> localReplicaSiloListPerRegion;

    /// <summary> region ID, silo ID, the list of PM IDs in a whole region </summary>
    [Key(2)]
    public Dictionary<string, Dictionary<string, List<Guid>>> pmList;

    /// <summary> region ID, the list of regional coordinators </summary>
    [Key(3)]
    public Dictionary<string, List<Guid>> regionalCoordList;

    /// <summary> region ID, local silo ID, list of local coordinators </summary>
    [Key(4)]
    public Dictionary<string, Dictionary<string, List<Guid>>> localCoordList;

    public StaticReplicaInfo()
    {
        regionList = new List<string>();
        localReplicaSiloListPerRegion = new Dictionary<string, List<string>>();
        pmList = new Dictionary<string, Dictionary<string, List<Guid>>>();
        regionalCoordList = new Dictionary<string, List<Guid>>();
        localCoordList = new Dictionary<string, Dictionary<string, List<Guid>>>();
    }

    public StaticReplicaInfo(List<string> regionList, Dictionary<string, List<string>> localReplicaSiloListPerRegion, Dictionary<string, Dictionary<string, List<Guid>>> pmList, Dictionary<string, List<Guid>> regionalCoordList, Dictionary<string, Dictionary<string, List<Guid>>> localCoordList)
    {
        this.regionList = regionList;
        this.localReplicaSiloListPerRegion = localReplicaSiloListPerRegion;
        this.pmList = pmList;
        this.regionalCoordList = regionalCoordList;
        this.localCoordList = localCoordList;
    }

    public void Init
    (
        int numReplicaSiloPerRegion,
        List<(SnapperRoleType, string, string)> roles,   // <role, region, ip>
        Dictionary<SnapperRoleType, int> roleSizes,      // <role type, #CPU>
        Dictionary<string, List<string>> replicaInfo,    // <region ID, list of local replica silo ip address>
        Dictionary<string, string> ipToSiloAddress       // <public ip, silo address>
    )
    {
        // =========================================================================================================================
        regionList = replicaInfo.Keys.ToList();

        // =========================================================================================================================
        regionList.ForEach(regionID =>
        {
            var localReplicaSiloList = roles.Where(x => x.Item1 == SnapperRoleType.LocalReplicaSilo && x.Item2 == regionID).Select(x => ipToSiloAddress[x.Item3]).ToList();
            Debug.Assert(localReplicaSiloList.Count == numReplicaSiloPerRegion);
            localReplicaSiloListPerRegion.Add(regionID, localReplicaSiloList);
        });

        // =========================================================================================================================
        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            pmList.Add(regionID, new Dictionary<string, List<Guid>>());

            for (var siloIndex = 0; siloIndex < numReplicaSiloPerRegion; siloIndex++)
            {
                var localSiloID = localReplicaSiloListPerRegion[regionID][siloIndex];
                pmList[regionID].Add(localSiloID, new List<Guid>());

                var numPMPerLocalSilo = Constants.numReplicaPlacementManagerPerCPU * roleSizes[SnapperRoleType.LocalSilo];
                for (var i = 0; i < numPMPerLocalSilo; i++) pmList[regionID][localSiloID].Add(Helper.ConvertIntToGuid(i));
            }
        }

        // =========================================================================================================================
        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            regionalCoordList.Add(regionID, new List<Guid>());
            var numRegionalCoordPerRegion = Constants.numRegionalCoordPerCPU * roleSizes[SnapperRoleType.RegionalSilo];
            for (int i = 0; i < numRegionalCoordPerRegion; i++) regionalCoordList[regionID].Add(Helper.ConvertIntToGuid(i));
        }

        // =========================================================================================================================
        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            localCoordList.Add(regionID, new Dictionary<string, List<Guid>>());

            for (var siloIndex = 0; siloIndex < numReplicaSiloPerRegion; siloIndex++)
            {
                var localSiloID = localReplicaSiloListPerRegion[regionID][siloIndex];
                localCoordList[regionID].Add(localSiloID, new List<Guid>());
                var numLocalCoordPerSilo = Constants.numCPUPerLocalSilo * Constants.numLocalCoordPerCPU;
                for (var i = 0; i < numLocalCoordPerSilo; i++)
                    localCoordList[regionID][localSiloID].Add(Helper.ConvertIntToGuid(i));
            }
        }
    }

    public void PrintContent()
    {
        Console.WriteLine();
        Console.WriteLine($"=========================================================================================");

        Console.WriteLine($"StaticReplicaInfo: regionList                    = {string.Join(", ", regionList)}");

        Console.WriteLine($"StaticReplicaInfo: localReplicaSiloListPerRegion = ");
        foreach (var item in localReplicaSiloListPerRegion)
            Console.WriteLine($"                       {item.Key} => {string.Join(", ", item.Value)}");

        Console.WriteLine($"StaticReplicaInfo: pmList                        = ");
        foreach (var item in pmList)
        {
            Console.WriteLine($"                       {item.Key} => ");
            foreach (var iitem in item.Value)
            {
                var list = iitem.Value.Select(Helper.ConvertGuidToInt).ToList();
                Console.WriteLine($"                           {iitem.Key} => {string.Join(", ", list)}");
            }
        }

        Console.WriteLine($"StaticReplicaInfo: regionalCoordList             = ");
        foreach (var item in regionalCoordList)
        {
            var list = item.Value.Select(Helper.ConvertGuidToInt).ToList();
            Console.WriteLine($"                       {item.Key} => {string.Join(", ", list)}");
        }

        Console.WriteLine($"StaticReplicaInfo: localCoordList                = ");
        foreach (var item in localCoordList)
        {
            Console.WriteLine($"                       {item.Key} => ");
            foreach (var iitem in item.Value)
            {
                var list = iitem.Value.Select(Helper.ConvertGuidToInt).ToList();
                Console.WriteLine($"                           {iitem.Key} => {string.Join(", ", list)}");
            }
        }

        Console.WriteLine($"=========================================================================================");
        Console.WriteLine();
    }
}
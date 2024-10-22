using MessagePack;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.Cache;

[MessagePackObject]
public class StaticClusterInfo
{
    [Key(0)]
    public string globalRegion;

    [Key(1)]
    public string globalSiloAddress;

    /// <summary> the list of regions (do not include the one that hosts the global silo) </summary>
    [Key(2)]
    public List<string> regionList;

    /// <summary> region ID, the regional silo ID </summary>
    [Key(3)]
    public Dictionary<string, string> regionalSiloPerRegion;

    /// <summary> region ID, the list of local silo IDs </summary>
    [Key(4)]
    public Dictionary<string, List<string>> localSiloListPerRegion;

    /// <summary> region ID, silo ID, the list of PM IDs in a whole region </summary>
    [Key(5)]
    public Dictionary<string, Dictionary<string, List<Guid>>> pmList;

    /// <summary> the list of global coordinators </summary>
    [Key(6)]
    public List<Guid> globalCoordList;

    /// <summary> region ID, the list of regional coordinators </summary>
    [Key(7)]
    public Dictionary<string, List<Guid>> regionalCoordList;

    /// <summary> region ID, local silo ID, list of local coordinators </summary>
    [Key(8)]
    public Dictionary<string, Dictionary<string, List<Guid>>> localCoordList;

    /// <summary> 
    /// determine how an ACT wait for batch of PACTs
    /// if true, an ACT can execute when previous batch completes, otherwise, wait for batch commit 
    /// </summary>
    [Key(9)]
    public bool speculativeACT;

    /// <summary> 
    /// determine how a batch of PACT wait for previous batch
    /// if true, a batch can execute when previous batch completes, otherwise, wait for batch commit 
    /// </summary>
    [Key(10)]
    public bool speculativeBatch;

    [Key(11)]
    public double globalBatchSizeInMSecs;

    [Key(12)]
    public double regionalBatchSizeInMSecs;

    [Key(13)]
    public double localBatchSizeInMSecs;

    /// <summary> region ID, silo ID (can be global / regional / local), list of migration workers </summary>
    [Key(14)]
    public Dictionary<string, Dictionary<string, List<Guid>>> mwList;

    public StaticClusterInfo()
    {
        globalRegion = "";
        globalSiloAddress = "";
        regionList = new List<string>();
        regionalSiloPerRegion = new Dictionary<string, string>();
        localSiloListPerRegion = new Dictionary<string, List<string>>();
        pmList = new Dictionary<string, Dictionary<string, List<Guid>>>();
        globalCoordList = new List<Guid>();
        regionalCoordList = new Dictionary<string, List<Guid>>();
        localCoordList = new Dictionary<string, Dictionary<string, List<Guid>>>();
        speculativeACT = false;
        speculativeBatch = false;
        globalBatchSizeInMSecs = 0;
        regionalBatchSizeInMSecs = 0;
        localBatchSizeInMSecs = 0;
        mwList = new Dictionary<string, Dictionary<string, List<Guid>>>();
    }

    public StaticClusterInfo(
        string globalRegion, 
        string globalSiloAddress, 
        List<string> regionList, 
        Dictionary<string, string> regionalSiloPerRegion, 
        Dictionary<string, List<string>> localSiloListPerRegion, 
        Dictionary<string, Dictionary<string, List<Guid>>> pmList, List<Guid> globalCoordList, 
        Dictionary<string, List<Guid>> regionalCoordList, 
        Dictionary<string, Dictionary<string, List<Guid>>> localCoordList, 
        bool speculativeACT, 
        bool speculativeBatch, 
        double globalBatchSizeInMSecs, 
        double regionalBatchSizeInMSecs, 
        double localBatchSizeInMSecs,
        Dictionary<string, Dictionary<string, List<Guid>>> mwList)
    {
        this.globalRegion = globalRegion;
        this.globalSiloAddress = globalSiloAddress;
        this.regionList = regionList;
        this.regionalSiloPerRegion = regionalSiloPerRegion;
        this.localSiloListPerRegion = localSiloListPerRegion;
        this.pmList = pmList;
        this.globalCoordList = globalCoordList;
        this.regionalCoordList = regionalCoordList;
        this.localCoordList = localCoordList;
        this.speculativeACT = speculativeACT;
        this.speculativeBatch = speculativeBatch;
        this.globalBatchSizeInMSecs = globalBatchSizeInMSecs;
        this.regionalBatchSizeInMSecs = regionalBatchSizeInMSecs;
        this.localBatchSizeInMSecs = localBatchSizeInMSecs;
        this.mwList = mwList;
    }

    public void Init
    (
        int numSiloPerRegion,
        List<(SnapperRoleType, string, string)> roles,   // <role, region, ip>
        Dictionary<SnapperRoleType, int> roleSizes,      // <role type, #CPU>
        Dictionary<string, List<string>> clusterInfo,    // <region ID, list of local silo ip address>
        Dictionary<string, string> ipToSiloAddress,      // <public ip, silo address>
        bool speculativeACT,
        bool speculativeBatch,
        double globalBatchSizeInMSecs,
        double regionalBatchSizeInMSecs,
        double localBatchSizeInMSecs
    )
    {
        // =========================================================================================================================
        globalRegion = roles.Find(x => x.Item1 == SnapperRoleType.GlobalSilo).Item2;
        globalSiloAddress = ipToSiloAddress[roles.Find(x => x.Item1 == SnapperRoleType.GlobalSilo).Item3];
        regionList = clusterInfo.Keys.ToList();

        // =========================================================================================================================
        regionList.ForEach(regionID =>
        {
            var regionalSiloID = ipToSiloAddress[roles.Find(x => x.Item1 == SnapperRoleType.RegionalSilo && x.Item2 == regionID).Item3];
            regionalSiloPerRegion.Add(regionID, regionalSiloID);
        });

        regionList.ForEach(regionID =>
        {
            var localSiloList = roles.Where(x => x.Item1 == SnapperRoleType.LocalSilo && x.Item2 == regionID).Select(x => ipToSiloAddress[x.Item3]).ToList();
            Debug.Assert(localSiloList.Count == numSiloPerRegion);
            localSiloListPerRegion.Add(regionID, localSiloList);
        });

        // =========================================================================================================================
        var numPMPerLocalSilo = Constants.numPlacementManaerPerCPU * roleSizes[SnapperRoleType.LocalSilo];
        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            pmList.Add(regionID, new Dictionary<string, List<Guid>>());
            
            for (var siloIndex = 0; siloIndex < numSiloPerRegion; siloIndex++)
            {
                var localSiloID = localSiloListPerRegion[regionID][siloIndex];
                pmList[regionID].Add(localSiloID, new List<Guid>());
                for (var i = 0; i < numPMPerLocalSilo; i++) pmList[regionID][localSiloID].Add(Helper.ConvertIntToGuid(i));
            }
        }

        // =========================================================================================================================
        for (var i = 0; i < Constants.numGlobalCoordPerCPU * roleSizes[SnapperRoleType.GlobalSilo]; i++) globalCoordList.Add(Helper.ConvertIntToGuid(i));

        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            regionalCoordList.Add(regionID, new List<Guid>());
            var numRegionalCoordPerRegion = Constants.numRegionalCoordPerCPU * roleSizes[SnapperRoleType.RegionalSilo];
            for (int i = 0; i < numRegionalCoordPerRegion; i++) regionalCoordList[regionID].Add(Helper.ConvertIntToGuid(i));
        }

        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            localCoordList.Add(regionID, new Dictionary<string, List<Guid>>());
            
            for (var siloIndex = 0; siloIndex < numSiloPerRegion; siloIndex++)
            {
                var localSiloID = localSiloListPerRegion[regionID][siloIndex];
                localCoordList[regionID].Add(localSiloID, new List<Guid>());
                var numLocalCoordPerSilo = Constants.numCPUPerLocalSilo * Constants.numLocalCoordPerCPU;
                for (var i = 0; i < numLocalCoordPerSilo; i++)
                    localCoordList[regionID][localSiloID].Add(Helper.ConvertIntToGuid(i));
            }
        }

        // =========================================================================================================================
        this.speculativeACT = speculativeACT;
        this.speculativeBatch = speculativeBatch;

        // todo: need to think how to scale (scale factor, may depend on #silo, #region)
        this.globalBatchSizeInMSecs = globalBatchSizeInMSecs;
        this.regionalBatchSizeInMSecs = regionalBatchSizeInMSecs;
        this.localBatchSizeInMSecs = localBatchSizeInMSecs;

        // =========================================================================================================================
        // migration worker on the global silo
        mwList.Add(globalRegion, new Dictionary<string, List<Guid>>());
        mwList[globalRegion].Add(globalSiloAddress, new List<Guid> { Helper.ConvertIntToGuid(0) });

        var numMWPerLocalSilo = Constants.numMigrationWorkerPerCPU * roleSizes[SnapperRoleType.LocalSilo];
        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            var regionalSiloAddress = regionalSiloPerRegion[regionID];

            // migration worker on each regional silo
            if (!mwList.ContainsKey(regionID)) mwList.Add(regionID, new Dictionary<string, List<Guid>>());
            mwList[regionID].Add(regionalSiloAddress, new List<Guid> { Helper.ConvertIntToGuid(0) });

            for (var siloIndex = 0; siloIndex < numSiloPerRegion; siloIndex++)
            {
                var localSiloID = localSiloListPerRegion[regionID][siloIndex];
                mwList[regionID].Add(localSiloID, new List<Guid>());
                for (var i = 0; i < numMWPerLocalSilo; i++) mwList[regionID][localSiloID].Add(Helper.ConvertIntToGuid(i));
            }
        }
    }

    public void PrintContent()
    {
        Console.WriteLine();
        Console.WriteLine($"=========================================================================================");

        Console.WriteLine($"StaticClusterInfo: globalRegion             = {globalRegion}");

        Console.WriteLine($"StaticClusterInfo: globalSiloAddress        = {globalSiloAddress}");

        Console.WriteLine($"StaticClusterInfo: regionList               = {string.Join(", ", regionList)}");

        Console.WriteLine($"StaticClusterInfo: regionalSiloPerRegion    = ");
        foreach (var item in regionalSiloPerRegion)  
            Console.WriteLine($"                       {item.Key} => {item.Value}");
        
        Console.WriteLine($"StaticClusterInfo: localSiloListPerRegion   = ");
        foreach (var item in localSiloListPerRegion) 
            Console.WriteLine($"                       {item.Key} => {string.Join(", ", item.Value)}");

        Console.WriteLine($"StaticClusterInfo: pmList                   = ");
        foreach (var item in pmList)
        {
            Console.WriteLine($"                       {item.Key} => ");
            foreach (var iitem in item.Value)
            {
                var list = iitem.Value.Select(Helper.ConvertGuidToInt).ToList();
                Console.WriteLine($"                           {iitem.Key} => {string.Join(", ", list)}");
            }   
        }

        var ll = globalCoordList.Select(Helper.ConvertGuidToInt).ToList();
        Console.WriteLine($"StaticClusterInfo: globalCoordList          = {string.Join(", ", ll)}");

        Console.WriteLine($"StaticClusterInfo: regionalCoordList        = ");
        foreach (var item in regionalCoordList)
        {
            var list = item.Value.Select(Helper.ConvertGuidToInt).ToList();
            Console.WriteLine($"                       {item.Key} => {string.Join(", ", list)}");
        }

        Console.WriteLine($"StaticClusterInfo: localCoordList           = ");
        foreach (var item in localCoordList)
        {
            Console.WriteLine($"                       {item.Key} => ");
            foreach (var iitem in item.Value)
            {
                var list = iitem.Value.Select(Helper.ConvertGuidToInt).ToList();
                Console.WriteLine($"                           {iitem.Key} => {string.Join(", ", list)}");
            }
        }

        Console.WriteLine($"StaticClusterInfo: speculativeACT           = {speculativeACT}");

        Console.WriteLine($"StaticClusterInfo: speculativeBatch         = {speculativeBatch}");

        Console.WriteLine($"StaticClusterInfo: globalBatchSizeInMSecs   = {globalBatchSizeInMSecs}");

        Console.WriteLine($"StaticClusterInfo: regionalBatchSizeInMSecs = {regionalBatchSizeInMSecs}");

        Console.WriteLine($"StaticClusterInfo: localBatchSizeInMSecs    = {localBatchSizeInMSecs}");

        Console.WriteLine($"StaticClusterInfo: mwList                   = ");
        foreach (var item in mwList)
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
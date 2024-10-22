using Concurrency.Common;
using Concurrency.Common.Cache;
using Concurrency.Common.ICache;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Experiment.Common;
using MessagePack;
using TestApp.Grains;
using System.Diagnostics;
using Utilities;
using Replication.Interface.TransactionReplication;
using Replication.Interface.GrainReplicaPlacement;
using Concurrency.Interface.DataModel;

namespace TestApp.Workload;

[MessagePackObject]
public class EnvConfigure : IEnvConfigure
{
    [Key(0)]
    public Dictionary<GrainID, int> grainIDToPartitionID;                         // grain ID => partition ID

    [Key(1)]
    public Dictionary<int, (string, string)> partitionIDToMasterInfo;             // partition ID => (master region ID, master silo ID)

    [Key(2)]
    public Dictionary<GrainID, int> replicaGrainIDToPartitionID;                  // replica grain ID => partition ID

    [Key(3)]
    public Dictionary<string, Dictionary<int, string>> partitionIDToReplicaInfo;  // region ID, partition ID => replica silo ID in the region

    [Key(4)]
    public Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker;          // grain ID => migration worker (guid, location) 

    Dictionary<string, Dictionary<string, List<GrainID>>> grains;                 // region ID, silo ID, grains

    Dictionary<string, Dictionary<string, List<GrainID>>> replicaGrains;          // region ID, silo ID, replica grains

    public EnvConfigure()
    {
        grainIDToPartitionID = new Dictionary<GrainID, int>();
        partitionIDToMasterInfo = new Dictionary<int, (string, string)>();
        replicaGrainIDToPartitionID = new Dictionary<GrainID, int>();
        partitionIDToReplicaInfo = new Dictionary<string, Dictionary<int, string>>();
        grainIDToMigrationWorker = new Dictionary<GrainID, (Guid, string)>();
        grains = new Dictionary<string, Dictionary<string, List<GrainID>>>();
        replicaGrains = new Dictionary<string, Dictionary<string, List<GrainID>>>();
    }

    public EnvConfigure(
        Dictionary<GrainID, int> grainIDToPartitionID, 
        Dictionary<int, (string, string)> partitionIDToMasterInfo, 
        Dictionary<GrainID, int> replicaGrainIDToPartitionID, 
        Dictionary<string, Dictionary<int, string>> partitionIDToReplicaInfo,
        Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker)
    {
        this.grainIDToPartitionID = grainIDToPartitionID;
        this.partitionIDToMasterInfo = partitionIDToMasterInfo;
        this.replicaGrainIDToPartitionID = replicaGrainIDToPartitionID;
        this.partitionIDToReplicaInfo = partitionIDToReplicaInfo;
        this.grainIDToMigrationWorker = grainIDToMigrationWorker;
        grains = new Dictionary<string, Dictionary<string, List<GrainID>>>();
        replicaGrains = new Dictionary<string, Dictionary<string, List<GrainID>>>();
    }

    public void GenerateGrainPlacementInfo(List<string> regionList, IEnvSetting iEnvSetting, StaticClusterInfo staticClusterInfo)
    {
        var envSetting = (EnvSetting)iEnvSetting;
        var basicEnvSetting = envSetting.GetBasic();

        var grainClassName = new GrainNameHelper().GetGrainClassName(basicEnvSetting.implementationType, false);
        for (var partitionID = 0; partitionID < envSetting.numMasterPartitionPerLocalSilo * basicEnvSetting.numSiloPerRegion * basicEnvSetting.numRegion; partitionID++)
        {
            var grains = Helper.GetMasterGrainsOfPartition(partitionID, envSetting.numGrainPerPartition);
            grains.ForEach(guid => grainIDToPartitionID.Add(new GrainID(guid, grainClassName), partitionID));
        }

        for (var partitionID = 0; partitionID < envSetting.numMasterPartitionPerLocalSilo * basicEnvSetting.numSiloPerRegion * basicEnvSetting.numRegion; partitionID++)
        {
            (var masterRegionIndex, var masterSiloIndex) = Helper.GetPartitionMasterInfo(partitionID, basicEnvSetting.numSiloPerRegion, envSetting.numMasterPartitionPerLocalSilo);
            var regionID = regionList[masterRegionIndex];
            var siloID = staticClusterInfo.localSiloListPerRegion[regionID][masterSiloIndex];
            partitionIDToMasterInfo.Add(partitionID, (regionID, siloID));
        }

        // assign each grain a migration worker
        var mwList = staticClusterInfo.mwList;
        foreach (var item in grainIDToPartitionID)
        {
            // select a migration worker in the same region and silo
            (var regionID, var siloID) = partitionIDToMasterInfo[item.Value];

            Debug.Assert(mwList.ContainsKey(regionID) && mwList[regionID].ContainsKey(siloID));
            var list = mwList[regionID][siloID];
            var selectedGuid = list[new Random().Next(0, list.Count)];
            grainIDToMigrationWorker.Add(item.Key, (selectedGuid, regionID + "+" + siloID));
        }
    }

    public void GenerateReplicaGrainPlacementInfo(List<string> regionList, IEnvSetting iEnvSetting, StaticReplicaInfo staticReplicaInfo)
    {
        var envSetting = (EnvSetting)iEnvSetting;
        var basicEnvSetting = envSetting.GetBasic();

        var grainClassName = new GrainNameHelper().GetGrainClassName(basicEnvSetting.implementationType, true);

        foreach (var item in grainIDToPartitionID) replicaGrainIDToPartitionID.Add(new GrainID(item.Key.id, grainClassName), item.Value);

        for (var partitionID = 0; partitionID < envSetting.numMasterPartitionPerLocalSilo * basicEnvSetting.numSiloPerRegion * basicEnvSetting.numRegion; partitionID++)
        {
            (var masterRegionIndex, var masterSiloIndex) = Helper.GetPartitionMasterInfo(partitionID, basicEnvSetting.numSiloPerRegion, envSetting.numMasterPartitionPerLocalSilo);

            if (basicEnvSetting.inRegionReplication)
            {
                // select the replica silo from the same region
                var regionID = regionList[masterRegionIndex];
                var siloID = staticReplicaInfo.localReplicaSiloListPerRegion[regionID][masterSiloIndex];
                if (!partitionIDToReplicaInfo.ContainsKey(regionID)) partitionIDToReplicaInfo.Add(regionID, new Dictionary<int, string>());
                partitionIDToReplicaInfo[regionID].Add(partitionID, siloID);
            }

            if (basicEnvSetting.crossRegionReplication)
            {
                // select the replica silo from the next region
                var regionID = regionList[(masterRegionIndex + 1) % regionList.Count];
                var siloID = staticReplicaInfo.localReplicaSiloListPerRegion[regionID][masterSiloIndex];
                if (!partitionIDToReplicaInfo.ContainsKey(regionID)) partitionIDToReplicaInfo.Add(regionID, new Dictionary<int, string>());
                partitionIDToReplicaInfo[regionID].Add(partitionID, siloID);
            }
        }
        Debug.Assert(partitionIDToReplicaInfo.Count == regionList.Count);

    }

    public (Dictionary<GrainID, int>, Dictionary<int, (string, string)>, Dictionary<GrainID, (Guid, string)>) GetGrainPlacementInfo() => (grainIDToPartitionID, partitionIDToMasterInfo, grainIDToMigrationWorker);

    public (Dictionary<GrainID, int>, Dictionary<string, Dictionary<int, string>>) GetReplicaGrainPlacementInfo() => (replicaGrainIDToPartitionID, partitionIDToReplicaInfo);
    
    public async Task InitAllGrains(string myRegion, string mySilo, IEnvSetting iEnvSetting, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory)
    {
        var batchSize = 100;
        var tasks = new List<Task>();
        var myPMListPerSilo = snapperClusterCache.GetPMList(myRegion);

        if (!grains.ContainsKey(myRegion)) grains.Add(myRegion, new Dictionary<string, List<GrainID>>());
        if (!grains[myRegion].ContainsKey(mySilo)) grains[myRegion].Add(mySilo, new List<GrainID>());

        var grainsInSilo = new List<GrainID>();
        foreach (var item in grainIDToPartitionID)
        {
            var actorID = item.Key;
            var partitionID = item.Value;
            (var regionID, var siloID) = partitionIDToMasterInfo[partitionID];
            if (regionID != myRegion || siloID != mySilo) continue;
            else grainsInSilo.Add(actorID);
        }
        grains[myRegion][mySilo] = grainsInSilo;

        // activate all grains first (set underMigration as false)
        foreach (var grainID in grainsInSilo)
        {
            var grain = grainFactory.GetGrain<ITransactionExecutionGrain>(grainID.id, myRegion + "+" + mySilo, grainID.className);
            tasks.Add(grain.ActivateGrain());

            if (tasks.Count % batchSize == 0)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
            }
        }
        await Task.WhenAll(tasks);
        tasks.Clear();
        Console.WriteLine($"Activate {grainsInSilo.Count} transactional actors. ");

        foreach (var id in grainsInSilo)
        {
            var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
            var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

            var method = typeof(SnapperTransactionalTestGrain).GetMethod("Init");
            Debug.Assert(method != null);
            var startFunc = new FunctionCall(method, id.id);

            tasks.Add(pm.SubmitTransaction(id, startFunc, new List<GrainID> { id }));
            //tasks.Add(pm.SubmitTransaction(id, startFunc));
            //await pm.SubmitTransaction(id, startFunc);
            //await pm.SubmitTransaction(id, startFunc, new List<GrainID> { id });

            if (tasks.Count % batchSize == 0)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
            }
        }
        await Task.WhenAll(tasks);
        Console.WriteLine($"Init {grainsInSilo.Count} transactional actors. ");
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory)
    {
        var result = new Dictionary<GrainID, (byte[], byte[], byte[], byte[])>();
        var myPMListPerSilo = snapperClusterCache.GetPMList(myRegion);

        var grainsInSilo = grains[myRegion][mySilo];

        foreach (var id in grainsInSilo)
        {
            var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
            var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

            var startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());

            var res = await pm.SubmitTransaction(id, startFunc);
            if (res.resultObj == null) Console.WriteLine($"exp = {res.exception}");
            Debug.Assert(res.resultObj != null);
            result.Add(id, ((byte[], byte[], byte[], byte[]))res.resultObj);
        }

        return result;
    }

    public async Task CheckGC(string myRegion, string mySilo, ImplementationType implementationType, IGrainFactory grainFactory)
    {
        var tasks = new List<Task>();
        var grainsInSilo = grains[myRegion][mySilo];
        foreach (var id in grainsInSilo)
        {
            switch (implementationType)
            {
                case ImplementationType.SNAPPERSIMPLE:
                case ImplementationType.SNAPPER:
                case ImplementationType.SNAPPERFINE:
                    var actor = grainFactory.GetGrain<ITransactionExecutionGrain>(id.id, myRegion + "+" + mySilo, id.className);
                    tasks.Add(actor.CheckGC());
                    break;
                case ImplementationType.NONTXNKV:
                    var actor1 = grainFactory.GetGrain<INonTransactionalKeyValueGrain>(id.id, myRegion + "+" + mySilo, id.className);
                    tasks.Add(actor1.CheckGC());
                    break;
            }
            
        }
        await Task.WhenAll(tasks);
    }

    public async Task InitAllReplicaGrains(string myRegion, string mySilo, IGrainFactory grainFactory)
    {
        if (!replicaGrains.ContainsKey(myRegion)) replicaGrains.Add(myRegion, new Dictionary<string, List<GrainID>>());
        if (!replicaGrains[myRegion].ContainsKey(mySilo)) replicaGrains[myRegion].Add(mySilo, new List<GrainID>());

        var replicaGrainsInSilo = new List<GrainID>();
        foreach (var item in replicaGrainIDToPartitionID)
        {
            var actorID = item.Key;
            var partitionID = item.Value;
            var siloID = partitionIDToReplicaInfo[myRegion][partitionID];
            if (siloID != mySilo) continue;
            else replicaGrainsInSilo.Add(actorID);
        }
        replicaGrains[myRegion][mySilo] = replicaGrainsInSilo;

        var batchSize = 100;
        var tasks = new List<Task>();
        foreach (var id in replicaGrainsInSilo)
        {
            var grain = grainFactory.GetGrain<ITransactionReplicationGrain>(id.id, myRegion + "+" + mySilo, id.className);
            tasks.Add(grain.Init());

            if (tasks.Count % batchSize == 0)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
            }
        }
        Console.WriteLine($"Init {replicaGrainsInSilo.Count} replica grains. ");
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllReplicaGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperReplicaCache snapperReplicaCache, IGrainFactory grainFactory)
    {
        var myPMListPerSilo = snapperReplicaCache.GetPMList(myRegion);
        var replicaGrainsInSilo = replicaGrains[myRegion][mySilo];

        var result = new Dictionary<GrainID, (byte[], byte[], byte[], byte[])>();
        foreach (var id in replicaGrainsInSilo)
        {
            var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
            var pm = grainFactory.GetGrain<IReplicaGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

            var startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());

            var res = await pm.SubmitTransaction(id, startFunc);
            Debug.Assert(res.resultObj != null);

            result.Add(id, ((byte[], byte[], byte[], byte[]))res.resultObj);
        }

        return result;
    }

    public async Task CheckGCForReplica(string myRegion, string mySilo, IGrainFactory grainFactory)
    {
        var replicaGrainsInSilo = replicaGrains[myRegion][mySilo];
        
        var tasks = new List<Task>();
        foreach (var id in replicaGrainsInSilo)
        {
            var actor = grainFactory.GetGrain<ITransactionReplicationGrain>(id.id, myRegion + "+" + mySilo, id.className);
            tasks.Add(actor.CheckGC());
        }
        await Task.WhenAll(tasks);
    }
}
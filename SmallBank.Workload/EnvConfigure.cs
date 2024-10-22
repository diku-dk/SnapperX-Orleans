using Concurrency.Common;
using Concurrency.Common.Cache;
using Concurrency.Common.ICache;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Experiment.Common;
using MessagePack;
using System.Diagnostics;
using Utilities;
using Replication.Interface.TransactionReplication;
using Replication.Interface.GrainReplicaPlacement;
using Concurrency.Interface.DataModel;
using SmallBank.Interfaces;
using System.Reflection;
using SmallBank.Grains;

namespace SmallBank.Workload;

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
        var envSetting = (EnvSetting)iEnvSetting;
        var basicEnvSetting = envSetting.GetBasic();
        var implementationType = basicEnvSetting.implementationType;
        var nameMap = new GrainNameHelper().GetNameMap(implementationType);

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

        var batchSize = 10;
        var tasks = new List<Task>();
        var myPMListPerSilo = snapperClusterCache.GetPMList(myRegion);

        if (implementationType == ImplementationType.SNAPPER ||
            implementationType == ImplementationType.SNAPPERSIMPLE ||
            implementationType == ImplementationType.SNAPPERFINE)
        {
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
        }

        var newTasks = new Dictionary<GrainID, Task<TransactionResult>>();
        foreach (var id in grainsInSilo)
        {
            var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
            var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

            var method = GrainNameHelper.GetMethod(implementationType, id.className, "Init");
            var startFunc = new FunctionCall(method, envSetting.numAccountPerGrain);

            switch (implementationType)
            {
                case ImplementationType.SNAPPERSIMPLE:
                case ImplementationType.SNAPPER:
                    Debug.Assert(method != null);
                    newTasks.Add(id, pm.SubmitTransaction(id, startFunc, new List<GrainID> { id }));
                    break;
                case ImplementationType.SNAPPERFINE:
                    Debug.Assert(method != null);
                    //await pm.SubmitTransaction(id, startFunc);
                    newTasks.Add(id, pm.SubmitTransaction(id, startFunc));
                    break;
                case ImplementationType.NONTXNKV:
                    Debug.Assert(method != null);
                    newTasks.Add(id, pm.SubmitNonTransactionalRequest(id, startFunc));
                    break;
                case ImplementationType.NONTXN:
                    var grain = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleAccountGrain>(snapperClusterCache, grainFactory, myRegion, id);
                    newTasks.Add(id, grain.Init(envSetting.numAccountPerGrain));
                    break;
                case ImplementationType.ORLEANSTXN:
                    var grain1 = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalAccountGrain>(snapperClusterCache, grainFactory, myRegion, id);
                    newTasks.Add(id, grain1.Init(envSetting.numAccountPerGrain));
                    break;
                default:
                    throw new Exception($"The implementationType {implementationType} is not supported. ");
            }

            if (newTasks.Count % batchSize == 0)
            {
                await Task.WhenAll(newTasks.Values);
                newTasks.Clear();
            }
        }
        await Task.WhenAll(newTasks.Values);

        foreach (var t in newTasks)
        {
            Debug.Assert(!t.Value.Result.hasException());
            if (implementationType == ImplementationType.ORLEANSTXN)
            {
                var grains = t.Value.Result.grains.Select(x => x.grainID).ToHashSet();
                Debug.Assert(grains.Contains(t.Key));
            }
        }

        Console.WriteLine($"Init {grainsInSilo.Count} transactional actors. ");
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory)
    {
        var result = new Dictionary<GrainID, (byte[], byte[], byte[], byte[])>();
        var myPMListPerSilo = snapperClusterCache.GetPMList(myRegion);

        var grainsInSilo = grains[myRegion][mySilo];

        foreach (var id in grainsInSilo)
        {
            MethodInfo? method = null;
            TransactionResult? res = null;
            FunctionCall? startFunc = null;
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                case ImplementationType.SNAPPERFINE:
                    var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                    var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

                    startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());
                    res = await pm.SubmitTransaction(id, startFunc);
                    break;
                case ImplementationType.NONTXNKV:
                    var pmID2 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                    var pm2 = grainFactory.GetGrain<IGrainPlacementManager>(pmID2, myRegion + "+" + mySilo);

                    startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());
                    res = await pm2.SubmitNonTransactionalRequest(id, startFunc);
                    break;
                case ImplementationType.SNAPPERSIMPLE:
                    var pmID1 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                    var pm1 = grainFactory.GetGrain<IGrainPlacementManager>(pmID1, myRegion + "+" + mySilo);

                    method = typeof(SnapperTransactionalSimpleAccountGrain).GetMethod("ReadSimpleState");
                    Debug.Assert(method != null);
                    startFunc = new FunctionCall(method);
                    res = await pm1.SubmitTransaction(id, startFunc);
                    break;
                case ImplementationType.ORLEANSTXN:
                    var accountGrain = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalAccountGrain>(snapperClusterCache, grainFactory, myRegion, id);
                    res = await accountGrain.ReadState();
                    break;
                case ImplementationType.NONTXN:
                    var accountGrain1 = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleAccountGrain>(snapperClusterCache, grainFactory, myRegion, id);
                    res = await accountGrain1.ReadState();
                    break;
            }

            Debug.Assert(res != null);
            if (res.resultObj == null)
            {
                Debug.Assert(res.hasException());
                Console.WriteLine($"exception = {res.exception}");
            }
            Debug.Assert(res.resultObj != null);
            result.Add(id, ((byte[], byte[], byte[], byte[]))res.resultObj);
        }

        return result;
    }

    public async Task<AggregatedExperimentData> CheckGC(string myRegion, string mySilo, ImplementationType implementationType, IGrainFactory grainFactory)
    {
        var tasks = new List<Task<AggregatedExperimentData>>();
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

        var data = new ExperimentData();
        foreach (var t in tasks) data.Set(t.Result);
        return data.AggregateAndClear();
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
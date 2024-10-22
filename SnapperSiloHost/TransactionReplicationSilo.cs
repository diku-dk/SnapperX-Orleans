using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Streams;
using Replication.Interface;
using Replication.Interface.Coordinator;
using Replication.Interface.GrainReplicaPlacement;
using Utilities;
using Concurrency.Common;
using System.Diagnostics;
using Experiment.Common;
using Concurrency.Common.Cache;

namespace SnapperSiloHost;

internal class TransactionReplicationSilo
{
    readonly SnapperRoleType myRole;
    readonly string myRegion;
    readonly string mySilo;

    readonly IEnvSetting envSetting;
    readonly BasicEnvSetting basicEnvSetting;
    readonly IHost siloHost;

    readonly StaticReplicaInfo staticReplicaInfo;
    readonly IEnvConfigure envConfigureHelper;

    readonly IGrainFactory grainFactory;
    readonly IHistoryManager historyManager;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly ISnapperReplicaCache snapperReplicaCache;
    readonly IClusterClient? orleansClient;
    
    public TransactionReplicationSilo(
        SnapperRoleType myRole, string myRegion, string mySilo, 
        IEnvSetting envSetting, IHost siloHost,
        StaticReplicaInfo staticReplicaInfo, IEnvConfigure envConfigureHelper)
    {
        this.myRole = myRole;
        this.myRegion = myRegion;
        this.mySilo = mySilo;

        this.envSetting = envSetting;
        this.basicEnvSetting = envSetting.GetBasic();
        this.siloHost = siloHost;

        this.staticReplicaInfo = staticReplicaInfo;
        this.envConfigureHelper = envConfigureHelper;

        snapperClusterCache = siloHost.Services.GetRequiredService<ISnapperClusterCache>();
        snapperReplicaCache = siloHost.Services.GetRequiredService<ISnapperReplicaCache>();
        grainFactory = siloHost.Services.GetRequiredService<IGrainFactory>();
        historyManager = siloHost.Services.GetRequiredService<IHistoryManager>();
        orleansClient = siloHost.Services.GetService<IClusterClient>();
    }

    public async Task Init()
    {
        // =========================================================================================================================
        (var replicaGrainIDToPartitionID, var partitionIDToReplicaInfo) = envConfigureHelper.GetReplicaGrainPlacementInfo();
        snapperReplicaCache.PrepareCache(staticReplicaInfo, replicaGrainIDToPartitionID, partitionIDToReplicaInfo[myRegion]);
        Console.WriteLine($"{myRole}: replica cache is prepared");

        // =========================================================================================================================
        // set up the history manager on this silo
        switch (myRole)
        {
            case SnapperRoleType.RegionalSilo:
                historyManager.Init(grainFactory, myRegion, mySilo, mySilo, Hierarchy.Regional);
                break;
            case SnapperRoleType.LocalReplicaSilo:
                var myRegionalSiloID = snapperClusterCache.GetRegionalSiloID(myRegion);
                historyManager.Init(grainFactory, myRegion, mySilo, myRegionalSiloID, Hierarchy.Local);
                break;
        }
        Console.WriteLine($"{myRole}: history manager is initiated");

        // =========================================================================================================================
        // subscribe to streams or kafka channels
        if (myRole == SnapperRoleType.LocalReplicaSilo)
        {
            IStreamProvider? streamProvider = null;
            if (basicEnvSetting.inRegionReplication) streamProvider = orleansClient.GetStreamProvider(Constants.DefaultStreamProvider);

            // each topic has one partition
            ConsumerConfig? kafkaConfig = null;
            if (basicEnvSetting.crossRegionReplication) kafkaConfig = new ConsumerConfig { BootstrapServers = basicEnvSetting.kafka_ConnectionString };

            var snapperSubscriber = siloHost.Services.GetRequiredService<ISnapperSubscriber>();
            await snapperSubscriber.Init(
                basicEnvSetting.inRegionReplication,
                basicEnvSetting.crossRegionReplication,
                envSetting.GetTotalNumMasterPartitions(),
                snapperReplicaCache.GetPMList(myRegion), 
                snapperReplicaCache.GetReplicaPartitionsInRegion(), 
                streamProvider, 
                kafkaConfig);
        }

        // =========================================================================================================================
        // init all SmallBank or TestApp grains on this silo, only including replicas
        if (myRole == SnapperRoleType.LocalReplicaSilo) await envConfigureHelper.InitAllReplicaGrains(myRegion, mySilo, grainFactory);
        
        // =========================================================================================================================
        // init all replica PMs
        if (myRole == SnapperRoleType.LocalReplicaSilo)
        {
            var tasks = new List<Task>();
            var pmIDs1 = snapperReplicaCache.GetPMListPerSilo(myRegion, mySilo);
            foreach (var pmID in pmIDs1)
            {
                var pm = grainFactory.GetGrain<IReplicaGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                tasks.Add(pm.Init());
            }
            await Task.WhenAll(tasks);
        }
    }

    public async Task<bool> CheckGC()
    {
        Console.WriteLine($"{myRole}: TransactionReplicationSilo check garbage collection...");

        var tasks = new List<Task>();
        switch (myRole)
        {
            case SnapperRoleType.RegionalSilo:
                // check all regional coordinators for transaction replication
                var regionalCoordIDs = snapperReplicaCache.GetAllRegionalCoords(myRegion);
                foreach (var regionalCoordID in regionalCoordIDs)
                {
                    var regionalCoord = grainFactory.GetGrain<IReplicaRegionalCoordGrain>(regionalCoordID, myRegion + "+" + mySilo);
                    tasks.Add(regionalCoord.CheckGC());
                }
                break;
            
            case SnapperRoleType.LocalReplicaSilo:
                // check all local coordinators for transaction replication
                var localCoordIDs1 = snapperReplicaCache.GetAllLocalCoords(myRegion, mySilo);
                foreach (var localCoordID in localCoordIDs1)
                {
                    var localCoord = grainFactory.GetGrain<IReplicaLocalCoordGrain>(localCoordID, myRegion + "+" + mySilo);
                    tasks.Add(localCoord.CheckGC());
                }
                await Task.WhenAll(tasks);

                // check all replica PM
                tasks.Clear();
                var pmIDs1 = snapperReplicaCache.GetPMListPerSilo(myRegion, mySilo);
                foreach (var pmID in pmIDs1)
                {
                    var pm = grainFactory.GetGrain<IReplicaGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                    tasks.Add(pm.CheckGC());
                }
                await Task.WhenAll(tasks);

                // check all replica actors
                await envConfigureHelper.CheckGCForReplica(myRegion, mySilo, grainFactory);
                
                break;
        }
        await Task.WhenAll(tasks);

        return historyManager.CheckGC();
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllGrainState()
    {
        Debug.Assert(myRole == SnapperRoleType.LocalReplicaSilo);
        return await envConfigureHelper.GetAllReplicaGrainState(myRegion, mySilo, basicEnvSetting.implementationType, snapperReplicaCache, grainFactory);
    }
}
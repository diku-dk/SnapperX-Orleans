using Concurrency.Interface;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Common;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Streams;
using System.Diagnostics;
using Utilities;
using Experiment.Common;
using Concurrency.Common.Cache;

namespace SnapperSiloHost;

internal class TransactionExecutionSilo
{
    readonly SnapperRoleType myRole;
    readonly string myRegion;
    readonly string mySilo;

    readonly IEnvSetting envSetting;
    readonly BasicEnvSetting basicEnvSetting;
    readonly IEnvConfigure envConfigureHelper;

    readonly ISnapperClusterCache snapperClusterCache;
    readonly IGrainFactory grainFactory;
    readonly IScheduleManager scheduleManager;
    readonly IClusterClient? orleansClient;
    readonly ISnapperLoggingHelper? snapperLoggingHelper;

    readonly StaticReplicaInfo? staticReplicaInfo;
    readonly ISnapperReplicaCache? snapperReplicaCache;

    public TransactionExecutionSilo(SnapperRoleType myRole, string myRegion, string mySilo, IEnvSetting envSetting, IEnvConfigure envConfigureHelper, IHost siloHost, StaticReplicaInfo? staticReplicaInfo)
    {
        this.myRole = myRole;
        this.myRegion = myRegion;
        this.mySilo = mySilo;

        this.envSetting = envSetting;
        this.basicEnvSetting = envSetting.GetBasic();

        this.envConfigureHelper = envConfigureHelper;

        snapperClusterCache = siloHost.Services.GetRequiredService<ISnapperClusterCache>();
        grainFactory = siloHost.Services.GetRequiredService<IGrainFactory>();
        orleansClient = siloHost.Services.GetService<IClusterClient>();
        
        if (basicEnvSetting.implementationType == ImplementationType.SNAPPER || 
            basicEnvSetting.implementationType == ImplementationType.SNAPPERSIMPLE || 
            basicEnvSetting.implementationType == ImplementationType.SNAPPERFINE ||
            basicEnvSetting.implementationType == ImplementationType.NONTXNKV)
        {
            scheduleManager = siloHost.Services.GetRequiredService<IScheduleManager>();
            snapperLoggingHelper = siloHost.Services.GetRequiredService<ISnapperLoggingHelper>();
        }
           
        if (myRole == SnapperRoleType.LocalSilo)
        {
            this.staticReplicaInfo = staticReplicaInfo;
            snapperReplicaCache = siloHost.Services.GetRequiredService<ISnapperReplicaCache>();
        }
    }

    public async Task Init()
    {
        // =========================================================================================================================
        if (myRole == SnapperRoleType.LocalSilo && staticReplicaInfo != null)
        {
            Debug.Assert(snapperReplicaCache != null);

            (var replicaGrainIDToPartitionID, var partitionIDToReplicaInfo) = envConfigureHelper.GetReplicaGrainPlacementInfo();
            snapperReplicaCache.PrepareCache(staticReplicaInfo, replicaGrainIDToPartitionID, partitionIDToReplicaInfo[myRegion]);
            Console.WriteLine($"{myRole}: replica cache is prepared");
        }

        // =========================================================================================================================
        // build Orleans clients to connect to other clusters
        var globalRegion = snapperClusterCache.GetGlobalRegion();
        var regionList = snapperClusterCache.GetRegionList();
        var allRegions = new HashSet<string> { globalRegion };
        foreach (var regionID in regionList) allRegions.Add(regionID);
        allRegions.Remove(myRegion);
        /*
        var clients = new Dictionary<string, List<IClusterClient>>();    // region ID, list of clients
        foreach (var regionID in allRegions)
        {
            clients.Add(regionID, new List<IClusterClient>());
            for (var i = 0; i < Constants.numCachedOrleansClient; i++)
            {
                var client = await OrleansClientManager.GetOrleansClient(regionID, basicEnvSetting.accessKey, basicEnvSetting.secretKey, basicEnvSetting.implementationType);
                clients[regionID].Add(client);
            }
        }
        snapperClusterCache.SetOrlenasClients(clients);
        Console.WriteLine($"{myRole}: initialize a set of Orleans clients for {allRegions.Count()} regions");
        */
        if (basicEnvSetting.implementationType == ImplementationType.SNAPPER || 
            basicEnvSetting.implementationType == ImplementationType.SNAPPERSIMPLE || 
            basicEnvSetting.implementationType == ImplementationType.SNAPPERFINE ||
            basicEnvSetting.implementationType == ImplementationType.NONTXNKV)
        {
            // =========================================================================================================================
            // set up the schedule manager on this silo
            switch (myRole)
            {
                case SnapperRoleType.GlobalSilo:
                    scheduleManager.Init(grainFactory, myRegion, mySilo, Hierarchy.Global);
                    break;
                case SnapperRoleType.RegionalSilo:
                    scheduleManager.Init(grainFactory, myRegion, mySilo, Hierarchy.Regional);
                    break;
                case SnapperRoleType.LocalSilo:
                    scheduleManager.Init(grainFactory, myRegion, mySilo, Hierarchy.Local);
                    break;
            }
            Console.WriteLine($"{myRole}: schedule manager is initiated");

            // =========================================================================================================================
            // set up all loggers or sub/pub event channels on this silo
            IStreamProvider? streamProvider = null;
            if (basicEnvSetting.inRegionReplication) streamProvider = orleansClient.GetStreamProvider(Constants.DefaultStreamProvider);

            // each topic has one partition
            ProducerConfig? kafkaConfig = null;
            if (basicEnvSetting.crossRegionReplication) kafkaConfig = new ProducerConfig { BootstrapServers = basicEnvSetting.kafka_ConnectionString, Partitioner = Partitioner.Consistent };

            if (snapperLoggingHelper != null)
            {
                var numMasterPartitionPerLocalSilo = envSetting.GetTotalNumMasterPartitions();
                await snapperLoggingHelper.Init(mySilo, basicEnvSetting.doLogging, basicEnvSetting.inRegionReplication, basicEnvSetting.crossRegionReplication, numMasterPartitionPerLocalSilo, streamProvider, kafkaConfig);
                Console.WriteLine($"{myRole}: init snapperLoggingHelper");
            }
        }

        if (basicEnvSetting.implementationType == ImplementationType.SNAPPERSIMPLE || 
            basicEnvSetting.implementationType == ImplementationType.SNAPPER || 
            basicEnvSetting.implementationType == ImplementationType.SNAPPERFINE)
            await InitCoordinators();
        
        // =========================================================================================================================
        // init all grains on this silo, only including master copies
        if (myRole == SnapperRoleType.LocalSilo) await envConfigureHelper.InitAllGrains(myRegion, mySilo, envSetting, snapperClusterCache, grainFactory);
        
        await CheckGC();
    }

    async Task InitCoordinators()
    {
        switch (myRole)
        {
            case SnapperRoleType.GlobalSilo:
                foreach (var guid in snapperClusterCache.GetAllGlobalCoords())
                {
                    var coord = grainFactory.GetGrain<IGlobalCoordGrain>(guid, myRegion + "+" + mySilo);
                    await coord.Init();
                }
                break;
            case SnapperRoleType.RegionalSilo:
                foreach (var guid in snapperClusterCache.GetAllRegionalCoords(myRegion))
                {
                    var coord = grainFactory.GetGrain<IRegionalCoordGrain>(guid, myRegion + "+" + mySilo);
                    await coord.Init();
                }
                break;
            case SnapperRoleType.LocalSilo:
                foreach (var guid in snapperClusterCache.GetAllLocalCoords(myRegion, mySilo))
                {
                    var coord = grainFactory.GetGrain<ILocalCoordGrain>(guid, myRegion + "+" + mySilo);
                    await coord.Init();
                }
                break;
        }
    }

    public async Task<AggregatedExperimentData> CheckGC()
    {
        if (basicEnvSetting.implementationType != ImplementationType.SNAPPER && 
            basicEnvSetting.implementationType != ImplementationType.SNAPPERSIMPLE && 
            basicEnvSetting.implementationType != ImplementationType.SNAPPERFINE &&
            basicEnvSetting.implementationType != ImplementationType.NONTXNKV) return new AggregatedExperimentData();

        Console.WriteLine($"{myRole}: TransactionExecutionSilo check garbage collection...");

        var data = new ExperimentData();
        data.Set(scheduleManager.CheckGC());

        var tasks = new List<Task<AggregatedExperimentData>>();
        switch (myRole)
        {
            case SnapperRoleType.GlobalSilo:
                // check all global coordinators
                var globalCoordIDs = snapperClusterCache.GetAllGlobalCoords();
                foreach (var globalCoordID in globalCoordIDs)
                {
                    var globalCoord = grainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID, myRegion + "+" + mySilo);
                    tasks.Add(globalCoord.CheckGC());
                }
                break;
            case SnapperRoleType.RegionalSilo:
                // check all regional coordinators for transaction execution
                var regionalCoordIDs = snapperClusterCache.GetAllRegionalCoords(myRegion);
                foreach (var regionalCoordID in regionalCoordIDs)
                {
                    var regionalCoord = grainFactory.GetGrain<IRegionalCoordGrain>(regionalCoordID, myRegion + "+" + mySilo);
                    tasks.Add(regionalCoord.CheckGC());
                }
                break;
            case SnapperRoleType.LocalSilo:
                // check all local coordinators for transaction execution
                var localCoordIDs = snapperClusterCache.GetAllLocalCoords(myRegion, mySilo);
                foreach (var localCoordID in localCoordIDs)
                {
                    var localCoord = grainFactory.GetGrain<ILocalCoordGrain>(localCoordID, myRegion + "+" + mySilo);
                    tasks.Add(localCoord.CheckGC());
                }
                await Task.WhenAll(tasks);

                // check all PM
                var pmIDs = snapperClusterCache.GetPMList(myRegion)[mySilo];
                foreach (var pmID in pmIDs)
                {
                    var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                    tasks.Add(pm.CheckGC());
                }
                await Task.WhenAll(tasks);

                // check all actors
                tasks.Add(envConfigureHelper.CheckGC(myRegion, mySilo, basicEnvSetting.implementationType, grainFactory));
                await Task.WhenAll(tasks);
                break;
        }
        await Task.WhenAll(tasks);

        foreach (var t in tasks) data.Set(t.Result);

        return data.AggregateAndClear();
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllGrainState()
    {
        Debug.Assert(myRole == SnapperRoleType.LocalSilo);
        return await envConfigureHelper.GetAllGrainState(myRegion, mySilo, basicEnvSetting.implementationType, snapperClusterCache, grainFactory);
    }
}
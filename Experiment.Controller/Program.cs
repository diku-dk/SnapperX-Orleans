using NetMQ;
using Utilities;
using NetMQ.Sockets;
using System.Diagnostics;
using Experiment.Worker;
using MessagePack;
using Confluent.Kafka;
using Concurrency.Common.Cache;
using Experiment.Common;
using Concurrency.Common.State;
using Concurrency.Common;
using StackExchange.Redis;
using MarketPlace.Grains;
using Experiment.Controller;
using SmallBank.Grains;

var myRole = SnapperRoleType.Controller;

// =========================================================================================================================
// set default values
var experimentID = "3";
var experimentIndex1 = 0;
var experimentIndex2 = 0;
var redis_ConnectionString = "localhost:" + Constants.redis;
var kafka_ConnectionString = "localhost:" + Constants.kafka;

// parse all input arguments
if (args.Length != 0)
{
    try
    {
        experimentID = args[0];
        experimentIndex1 = int.Parse(args[1]);
        experimentIndex2 = int.Parse(args[2]);
        redis_ConnectionString = args[3];
        kafka_ConnectionString = args[4];
    }
    catch (Exception e)
    {
        Console.WriteLine($"{e.Message}, {e.StackTrace}");
        throw;
    }
}
Console.WriteLine();
Console.WriteLine($"=========================================================================================");
Console.WriteLine();
Console.WriteLine($"{myRole}: experimentID = {experimentID}, index1 = {experimentIndex1}, index2 = {experimentIndex2}");

// =========================================================================================================================
var envSetting = EnvConfigurationParser.GetConfigurationFromXMLFile(experimentID, experimentIndex1, experimentIndex2);
var basicEnvSetting = envSetting.GetBasic();

var accessKey = "";
var secretKey = "";
var globalRegion = "";
var regionList = new List<string>();    // do not include the global region
using (var file = new StreamReader(Constants.credentialFile))
{
    accessKey = file.ReadLine();
    secretKey = file.ReadLine();
    globalRegion = file.ReadLine();

    for (var i = 0; i < basicEnvSetting.numRegion; i++)
    {
        var region = file.ReadLine();
        Debug.Assert(!string.IsNullOrEmpty(region));
        regionList.Add(region);
    }
}
Debug.Assert(!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey) && !string.IsNullOrEmpty(globalRegion));
basicEnvSetting.SetCredentialInfo(redis_ConnectionString, kafka_ConnectionString, accessKey, secretKey);
envSetting.PrintContent();

// =========================================================================================================================
// delete the DynamoDb table in every related region
var allRegions = new HashSet<string> { globalRegion };
regionList.ForEach(regionID => allRegions.Add(regionID));
foreach (var regionID in allRegions) await AWSClientManager.DeleteMembershipTable(myRole, regionID, accessKey, secretKey);
Console.WriteLine($"{myRole}: the DynamoDB tables are all deleted in {allRegions.Count} regions");

// =========================================================================================================================
// clean up Redis cache
var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { redis_ConnectionString }, AllowAdmin = true });
var server = redis.GetServer(redis_ConnectionString);
server.FlushAllDatabases();
Console.WriteLine($"{myRole}: The data in Redis cluster is all deleted");

// =========================================================================================================================
// delete the local log folders
if (basicEnvSetting.isLocalTest && Directory.Exists(Constants.logPath))
{
    Directory.Delete(Constants.logPath, true);
    Console.WriteLine($"{myRole}: the log folder is deleted");
}

// =========================================================================================================================
// clean up all events left in kafka channels by deleting the topics
if (basicEnvSetting.crossRegionReplication)
{
    // get all topics
    var topics = new List<string>();
    for (var partitionID = 0; partitionID < envSetting.GetTotalNumMasterPartitions(); partitionID++)
    {
        for (var index = 0; index < Constants.numEventChannelsPerPartition; index++)
        {
            topics.Add(SnapperLogType.Info.ToString() + "-" + partitionID.ToString() + "-" + index.ToString());
            topics.Add(SnapperLogType.Prepare.ToString() + "-" + partitionID.ToString() + "-" + index.ToString());
        }
    }

    // delete all topicss
    var kafkaClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka_ConnectionString }).Build();
    await kafkaClient.DeleteTopicsAsync(topics);
    Console.WriteLine($"{myRole}: delete all kafka topics");
}

// =========================================================================================================================
// set up socket connections to workers and silos
PullSocket pullSocket;
PublisherSocket publishSocket;
if (basicEnvSetting.isLocalTest)
{
    pullSocket = new PullSocket("@tcp://localhost:" + Constants.controllerPullPort);
    publishSocket = new PublisherSocket("@tcp://localhost:" + Constants.controllerPublishPort);
}
else 
{
    pullSocket = new PullSocket("@tcp://" + Helper.GetLocalIPAddress() + ":" + Constants.controllerPullPort);
    publishSocket = new PublisherSocket("@tcp://*:" + Constants.controllerPublishPort);
}

// =========================================================================================================================
NetworkMessage msg;
var clusterInfo = new Dictionary<string, List<string>>();                        // region ID, <local silo ip>
var replicaInfo = new Dictionary<string, List<string>>();                        // region ID, <local replica silo ip>
var workerInfo = new Dictionary<string, List<string>>();                         // region ID, <worker ip>
var replicaWorkerInfo = new Dictionary<string, List<string>>();                  // region ID, <replica worker ip>
var roles = new List<(SnapperRoleType, string, string)>();                       // role, region, ip

var numWorker = basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion;                // one worker per local silo 
var numReplicaWorker = basicEnvSetting.numRegion * basicEnvSetting.numReplicaSiloPerRegion;  // one worker per local replica silo
var totalNumWorker = numWorker;
if (basicEnvSetting.replicaWorkload) totalNumWorker += numReplicaWorker;

var totalNumSilo = 1 +                                                                    // global silo     
    basicEnvSetting.numRegion +                                                           // regional silo
    basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion;                         // local silo
if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication) 
    totalNumSilo += basicEnvSetting.numRegion * basicEnvSetting.numReplicaSiloPerRegion;  // local replica silo

Console.WriteLine($"{myRole}: expect to be connected by {totalNumSilo} silos and {totalNumWorker} workers");

for (var i = 0; i < totalNumWorker + totalNumSilo; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.CONNECT);
    
    (var role, var region, var ipAddress) = MessagePackSerializer.Deserialize<(SnapperRoleType, string, string)>(msg.content);
    if (role == SnapperRoleType.GlobalSilo) Debug.Assert(globalRegion == region);
    else Debug.Assert(regionList.Contains(region));

    Console.WriteLine($"{myRole}: connected by {role}");
    roles.Add((role, region, ipAddress));
    if (role == SnapperRoleType.LocalSilo)
    {
        if (!clusterInfo.ContainsKey(region)) clusterInfo.Add(region, new List<string>());
        clusterInfo[region].Add(ipAddress);
    }
    else if (role == SnapperRoleType.LocalReplicaSilo)
    {
        if (!replicaInfo.ContainsKey(region)) replicaInfo.Add(region, new List<string>());
        replicaInfo[region].Add(ipAddress);
    }
    else if (role == SnapperRoleType.Worker)
    {
        if (!workerInfo.ContainsKey(region)) workerInfo.Add(region, new List<string>());
        workerInfo[region].Add(ipAddress);
    }
    else if (role == SnapperRoleType.ReplicaWorker)
    {
        if (!replicaWorkerInfo.ContainsKey(region)) replicaWorkerInfo.Add(region, new List<string>());
        replicaWorkerInfo[region].Add(ipAddress);
    }
}
Console.WriteLine($"{myRole}: connected by {totalNumWorker} workers and {totalNumSilo} silos");

// =========================================================================================================================
var roleInfo = new Dictionary<string, (int, int)>();   // public ip, regionIndex, partitionIndex
roles.ForEach(x =>
{
    var regionIndex = -1;
    var siloIndex = -1;

    switch (x.Item1)
    {
        case SnapperRoleType.GlobalSilo:
            regionIndex = -1;
            siloIndex = -1;
            break;
        case SnapperRoleType.RegionalSilo:
            regionIndex = regionList.IndexOf(x.Item2);
            siloIndex = -1;
            break;
        case SnapperRoleType.LocalSilo:
            regionIndex = regionList.IndexOf(x.Item2);
            siloIndex = clusterInfo[x.Item2].IndexOf(x.Item3);
            break;
        case SnapperRoleType.LocalReplicaSilo:
            regionIndex = regionList.IndexOf(x.Item2);
            siloIndex = replicaInfo[x.Item2].IndexOf(x.Item3);
            break;
        case SnapperRoleType.Worker:
            regionIndex = regionList.IndexOf(x.Item2);
            siloIndex = workerInfo[x.Item2].IndexOf(x.Item3);
            break;
        case SnapperRoleType.ReplicaWorker:
            regionIndex = regionList.IndexOf(x.Item2);
            siloIndex = replicaWorkerInfo[x.Item2].IndexOf(x.Item3);
            break;
    }
    roleInfo.Add(x.Item3, (regionIndex, siloIndex));
});
Debug.Assert(roleInfo.Count == totalNumWorker + totalNumSilo);

// get the number of CPUs for each role type
var roleSizes = Helper.CalculateRoleSizes(basicEnvSetting.numRegion, basicEnvSetting.numSiloPerRegion);
basicEnvSetting.SetClusterInfo(roleSizes, roleInfo);

// =========================================================================================================================
byte[] serializedEnvSetting;
switch (basicEnvSetting.benchmark)
{
    case BenchmarkType.SMALLBANK:
        serializedEnvSetting = MessagePackSerializer.Serialize(envSetting as SmallBank.Workload.EnvSetting);
        break;
    case BenchmarkType.MARKETPLACE:
        serializedEnvSetting = MessagePackSerializer.Serialize(envSetting as MarketPlace.Workload.EnvSetting);
        break;
    default:
        throw new Exception($"The {basicEnvSetting.benchmark} benchamrk is not supported. ");
}

msg = new NetworkMessage(NetMsgType.ENV_SETTING, MessagePackSerializer.Serialize((basicEnvSetting.benchmark, serializedEnvSetting)));
publishSocket.SendMoreFrame(NetMsgType.ENV_SETTING.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.ENV_SETTING}");

// =========================================================================================================================
for (var i = 0; i < totalNumWorker + totalNumSilo; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
}
Console.WriteLine($"{myRole}: receives all ACKs");

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.START_GLOBAL_SILO);
publishSocket.SendMoreFrame(NetMsgType.START_GLOBAL_SILO.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.START_GLOBAL_SILO}");

var ipToSiloAddress = new Dictionary<string, string>();        // <public ip, silo address>
msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.ACK);
(var ip, var siloAddress) = MessagePackSerializer.Deserialize<(string, string)>(msg.content);
ipToSiloAddress.Add(ip, siloAddress);
Console.WriteLine($"{myRole}: receive ACK from the global silo");

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.START_REGIONAL_SILO);
publishSocket.SendMoreFrame(NetMsgType.START_REGIONAL_SILO.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.START_REGIONAL_SILO}");

for (var i = 0; i < basicEnvSetting.numRegion; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
    (ip, siloAddress) = MessagePackSerializer.Deserialize<(string, string)>(msg.content);
    ipToSiloAddress.Add(ip, siloAddress);
}
Console.WriteLine($"{myRole}: receive ACKs from {basicEnvSetting.numRegion} regional silos");

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.START_OTHER_SILO);
publishSocket.SendMoreFrame(NetMsgType.START_OTHER_SILO.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.START_OTHER_SILO}");

for (var i = 0; i < totalNumSilo - 1 - basicEnvSetting.numRegion; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
    (ip, siloAddress) = MessagePackSerializer.Deserialize<(string, string)>(msg.content);
    ipToSiloAddress.Add(ip, siloAddress);
}
Console.WriteLine($"{myRole}: receive ACKs from {totalNumSilo - 1 - basicEnvSetting.numRegion} local silos and local replica silos");

// =========================================================================================================================
var staticClusterInfo = new StaticClusterInfo();
staticClusterInfo.Init(basicEnvSetting.numSiloPerRegion, roles, roleSizes, clusterInfo, ipToSiloAddress,
    basicEnvSetting.speculativeACT, basicEnvSetting.speculativeBatch,
    basicEnvSetting.globalBatchSizeInMSecs, basicEnvSetting.regionalBatchSizeInMSecs, basicEnvSetting.localBatchSizeInMSecs);
staticClusterInfo.PrintContent();

var clusterInfo_db = redis.GetDatabase(Constants.Redis_ClusterInfo);
clusterInfo_db.StringSet("clusterInfo", MessagePackSerializer.Serialize(staticClusterInfo));
Console.WriteLine($"{myRole}: init clusterInfo and write it to Redis");

// =========================================================================================================================
IEnvConfigure? envConfigureHelper;
switch (basicEnvSetting.benchmark)
{
    case BenchmarkType.SMALLBANK:
        envConfigureHelper = new SmallBank.Workload.EnvConfigure();
        break;
    case BenchmarkType.MARKETPLACE:
        envConfigureHelper = new MarketPlace.Workload.EnvConfigure();
        break;
    default:
        throw new Exception($"The benchmark {basicEnvSetting.benchmark} is not supported. ");
}

envConfigureHelper.GenerateGrainPlacementInfo(regionList, envSetting, staticClusterInfo);
Console.WriteLine($"{myRole}: staticClusterInfo and grain placement info are prepared");

// =========================================================================================================================
if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication)
{
    var staticReplicaInfo = new StaticReplicaInfo();
    staticReplicaInfo.Init(basicEnvSetting.numReplicaSiloPerRegion, roles, roleSizes, replicaInfo, ipToSiloAddress);
    staticReplicaInfo.PrintContent();

    var replicaInfo_db = redis.GetDatabase(Constants.Redis_ReplicaInfo);
    replicaInfo_db.StringSet("replicaInfo", MessagePackSerializer.Serialize(staticReplicaInfo));
    Console.WriteLine($"{myRole}: init replicaInfo and write it to Redis");

    envConfigureHelper.GenerateReplicaGrainPlacementInfo(regionList, envSetting, staticReplicaInfo);
    Console.WriteLine($"{myRole}: staticReplicaInfo and replica grain placement info are prepared");
}

byte[] serializedEnvConfigureInfo;
switch (basicEnvSetting.benchmark)
{
    case BenchmarkType.SMALLBANK:
        serializedEnvConfigureInfo = MessagePackSerializer.Serialize((SmallBank.Workload.EnvConfigure)envConfigureHelper);
        break;
    case BenchmarkType.MARKETPLACE:
        serializedEnvConfigureInfo = MessagePackSerializer.Serialize((MarketPlace.Workload.EnvConfigure)envConfigureHelper);
        break;
    default:
        throw new Exception($"The benchmark {basicEnvSetting.benchmark} is not supported. ");
}

var envConfigureInfo_db = redis.GetDatabase(Constants.Redis_EnvConfigureInfo);
await envConfigureInfo_db.StringSetAsync("envConfigureInfo", serializedEnvConfigureInfo);
Console.WriteLine($"{myRole}: envConfigureInfo is written to Redis, size = {serializedEnvConfigureInfo.Length}");

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.ENV_INIT);
publishSocket.SendMoreFrame(NetMsgType.ENV_INIT.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.ENV_INIT}");

// =========================================================================================================================
for (var i = 0; i < totalNumWorker + 1 + basicEnvSetting.numRegion; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
}
Console.WriteLine($"{myRole}: receives ACKs from {totalNumWorker} workers + 1 global silo + {basicEnvSetting.numRegion} regional silos");

if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication)
{
    // =========================================================================================================================
    msg = new NetworkMessage(NetMsgType.INIT_REPLICA_SILO);
    publishSocket.SendMoreFrame(NetMsgType.INIT_REPLICA_SILO.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
    Console.WriteLine($"{myRole}: publish {NetMsgType.INIT_REPLICA_SILO}");

    // =========================================================================================================================
    for (var i = 0; i < basicEnvSetting.numRegion * basicEnvSetting.numReplicaSiloPerRegion; i++)
    {
        msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
        Debug.Assert(msg.msgType == NetMsgType.ACK);
    }
    Console.WriteLine($"{myRole}: receives ACKs from {basicEnvSetting.numRegion * basicEnvSetting.numReplicaSiloPerRegion} local replica silos");
}

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.INIT_LOCAL_SILO);
publishSocket.SendMoreFrame(NetMsgType.INIT_LOCAL_SILO.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.INIT_LOCAL_SILO}");

// =========================================================================================================================
for (var i = 0; i < basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
}
Console.WriteLine($"{myRole}: receives ACKs from {basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion} local silos");

// =========================================================================================================================
var workloadGroup = new List<IWorkloadConfigure>();
var replicaWorkloadGroup = new List<IWorkloadConfigure>();
switch (basicEnvSetting.benchmark)
{
    case BenchmarkType.SMALLBANK:
        workloadGroup = SmallBank.Workload.WorkloadParser.ParseWorkloadFromXMLFile(experimentID, basicEnvSetting.isGrainMigrationExp, envSetting);
        if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication) replicaWorkloadGroup = SmallBank.Workload.WorkloadParser.ParseReplicaWorkloadFromXMLFile(experimentID, basicEnvSetting.isGrainMigrationExp, basicEnvSetting.implementationType);
        break;
    case BenchmarkType.MARKETPLACE:
        workloadGroup = MarketPlace.Workload.WorkloadParser.ParseWorkloadFromXMLFile(experimentID, basicEnvSetting.isGrainMigrationExp, basicEnvSetting.implementationType);
        if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication) replicaWorkloadGroup = MarketPlace.Workload.WorkloadParser.ParseReplicaWorkloadFromXMLFile(experimentID, basicEnvSetting.isGrainMigrationExp, basicEnvSetting.implementationType);
        break;
    default:
        throw new Exception($"The benchmark {basicEnvSetting.benchmark} is not supported. ");
}

var numExperiment = replicaWorkloadGroup.Count == 0 ? workloadGroup.Count : workloadGroup.Count * replicaWorkloadGroup.Count;
msg = new NetworkMessage(NetMsgType.WORKLOAD_INIT, MessagePackSerializer.Serialize(numExperiment));
publishSocket.SendMoreFrame(NetMsgType.WORKLOAD_INIT.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.WORKLOAD_INIT}, numExperiment = {numExperiment}");

for (var i = 0; i < totalNumWorker + totalNumSilo; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
}
Console.WriteLine($"{myRole}: receives {totalNumWorker} ACKs from all workers and silos");

// =========================================================================================================================
(var numEpoch, var numReRun) = basicEnvSetting.GetNumEpochAndNumReRun();

var numReplicaWorkload = replicaWorkloadGroup.Count == 0 ? 1 : replicaWorkloadGroup.Count;
for (var i = 0; i < workloadGroup.Count; i++)
{
    for (var r = 0; r < numReplicaWorkload; r++)
    {
        for (var j = 0; j < numReRun; j++)
        {
            Console.WriteLine($"{myRole}: run workload {i + 1}/{workloadGroup.Count}, replica workload {r + 1}/{replicaWorkloadGroup.Count} for {j + 1}/{numReRun}th time");

            // send the workload config to workers
            byte[] workload;
            byte[] replicaWorkload;

            switch (basicEnvSetting.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    workload = MessagePackSerializer.Serialize(workloadGroup[i] as SmallBank.Workload.WorkloadConfigure);
                    if (replicaWorkloadGroup.Count != 0) replicaWorkload = MessagePackSerializer.Serialize(replicaWorkloadGroup[r] as SmallBank.Workload.WorkloadConfigure);
                    else replicaWorkload = Array.Empty<byte>();
                    break;
                case BenchmarkType.MARKETPLACE:
                    workload = MessagePackSerializer.Serialize(workloadGroup[i] as MarketPlace.Workload.WorkloadConfigure);
                    if (replicaWorkloadGroup.Count != 0) replicaWorkload = MessagePackSerializer.Serialize(replicaWorkloadGroup[r] as MarketPlace.Workload.WorkloadConfigure);
                    else replicaWorkload = Array.Empty<byte>();
                    break;
                default:
                    throw new Exception($"The benchmark {basicEnvSetting.benchmark} is not supported. ");
            }
            
            msg = new NetworkMessage(NetMsgType.WORKLOAD_CONFIG, MessagePackSerializer.Serialize((workload, replicaWorkload)));
            publishSocket.SendMoreFrame(NetMsgType.WORKLOAD_CONFIG.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
            Console.WriteLine($"{myRole}: publish {NetMsgType.WORKLOAD_CONFIG}, workload");
            for (var k = 0; k < totalNumWorker; k++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
                Debug.Assert(msg.msgType == NetMsgType.ACK);
            }
            Console.WriteLine($"{myRole}: receives {totalNumWorker} ACKs from all workers");

            // start to run epochs
            var resultAggregator = new ExperimentResultAggregator(false, envSetting, basicEnvSetting.implementationType, numEpoch, numWorker, workloadGroup[i], basicEnvSetting.benchmark);
            ExperimentResultAggregator? replicaResultAddregator = null; 
            if (replicaWorkloadGroup.Count != 0) replicaResultAddregator = new ExperimentResultAggregator(true, envSetting, basicEnvSetting.implementationType, numEpoch, numReplicaWorker, replicaWorkloadGroup[i], basicEnvSetting.benchmark);
            for (var epoch = 0; epoch < numEpoch; epoch++)
            {
                msg = new NetworkMessage(NetMsgType.RUN_EPOCH, MessagePackSerializer.Serialize(epoch));
                publishSocket.SendMoreFrame(NetMsgType.RUN_EPOCH.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
                Console.WriteLine($"{myRole}: publish {NetMsgType.RUN_EPOCH}, epoch = {epoch}");

                // receive experiment result from all workers
                var workerCount = 0;
                var replicaWorkerCount = 0;
                for (var k = 0; k < totalNumWorker; k++)
                {
                    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
                    Debug.Assert(msg.msgType == NetMsgType.ACK);
                    (var role, var result) = MessagePackSerializer.Deserialize<(SnapperRoleType, WorkloadResult)>(msg.content);
                    switch (role)
                    {
                        case SnapperRoleType.Worker:
                            resultAggregator.SetResult(epoch, workerCount++, result);
                            break;
                        case SnapperRoleType.ReplicaWorker:
                            if (replicaWorkloadGroup.Count != 0)
                            {
                                Debug.Assert(replicaResultAddregator != null);
                                replicaResultAddregator.SetResult(epoch, replicaWorkerCount++, result);
                            } 
                            break;
                        default: throw new Exception("This should never happen");
                    }
                }
                Console.WriteLine($"{myRole}: receives {totalNumWorker} ACKs from all workers");
            }

            Console.WriteLine($"{myRole}: Aggregate results and print");
            var success = resultAggregator.AggregateResultsAndPrint(j, numReRun);

            if (basicEnvSetting.replicaWorkload)
            {
                Console.WriteLine($"{myRole}: Aggregate replica results and print");
                success &= replicaResultAddregator.AggregateResultsAndPrint(j, numReRun);
            }
                
            if (success) break;
            Thread.Sleep(5000);
        }
    }
}

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.FINISH_EXP);
publishSocket.SendMoreFrame(NetMsgType.FINISH_EXP.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.FINISH_EXP}");

// =========================================================================================================================
var experimentData = new ExperimentData();
var masterGrains = new Dictionary<GrainID, (byte[], byte[], byte[], byte[])>();
var replicaGrains = new List<(GrainID, (byte[], byte[], byte[], byte[]))>();
for (var i = 0; i < totalNumSilo; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);

    (var role, var data, var aggregatedExperimentData) = MessagePackSerializer.Deserialize<(SnapperRoleType, byte[], AggregatedExperimentData)>(msg.content);
    experimentData.Set(aggregatedExperimentData);

    if (data.Length == 0) continue;
    switch (role)
    {
        case SnapperRoleType.LocalSilo:
            var masterGrainState = MessagePackSerializer.Deserialize<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>>(data);
            foreach (var item in masterGrainState)
            {
                Debug.Assert(!masterGrains.ContainsKey(item.Key));
                masterGrains.Add(item.Key, item.Value);
            }
            break;
        case SnapperRoleType.LocalReplicaSilo:
            var replicaGrainState = MessagePackSerializer.Deserialize<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>>(data);
            foreach (var item in replicaGrainState) replicaGrains.Add((item.Key, item.Value));
            break;
    }
}
Console.WriteLine($"{myRole}: receives {totalNumSilo} ACKs from all silos");

// =========================================================================================================================
var agg = experimentData.AggregateAndClear();
agg.Print();

// =========================================================================================================================
msg = new NetworkMessage(NetMsgType.TERMINATE);
publishSocket.SendMoreFrame(NetMsgType.TERMINATE.ToString()).SendFrame(MessagePackSerializer.Serialize(msg));
Console.WriteLine($"{myRole}: publish {NetMsgType.TERMINATE}");

// =========================================================================================================================
for (var i = 0; i < totalNumSilo; i++)
{
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(pullSocket.ReceiveFrameBytes());
    Debug.Assert(msg.msgType == NetMsgType.ACK);
}
Console.WriteLine($"{myRole}: receives {totalNumSilo} ACKs from all silos");

// =========================================================================================================================
// delete the DynamoDb table in every related region
foreach (var regionID in allRegions) await AWSClientManager.DeleteMembershipTable(myRole, regionID, accessKey, secretKey);
Console.WriteLine($"{myRole}: DynamoDB tables are all deleted in {allRegions.Count} regions");

// =========================================================================================================================
// check if the final grain state matches its replica

return;

switch (basicEnvSetting.implementationType)
{
    case ImplementationType.NONTXN:
    case ImplementationType.NONTXNKV:
        //CountAnomalies();
        break;
    case ImplementationType.ORLEANSTXN:
    case ImplementationType.SNAPPERSIMPLE:
        switch (basicEnvSetting.benchmark)
        {
            case BenchmarkType.MARKETPLACE:
                MarketPlaceSerializer.CheckStateCorrectnessForOrleans(masterGrains);
                break;
            case BenchmarkType.SMALLBANK:
                SmallBankSerializer.CheckStateCorrectnessForOrleans(masterGrains);
                break;
        }
        break;
    case ImplementationType.SNAPPER:
    case ImplementationType.SNAPPERFINE:
        CheckStateCorrectnessForSnapper();
        break;
}

void CheckStateCorrectnessForSnapper()
{
    var newMasterGrains = new Dictionary<GrainID, SnapperGrainState>();
    var newReplicaGrains = new List<(GrainID, SnapperGrainState)>();

    var masterKeyInfo = new Dictionary<GrainID, Dictionary<ISnapperKey, SnapperValue>>();  // (origin, key), value
    var followerKeyInfo = new List<(GrainID, ISnapperKey, SnapperValue)>();                // (copy, key, value), info of the key that has registered reference
    var referenceInfo = new Dictionary<SnapperKeyReferenceType, Dictionary<GrainID, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>>  // (type, grain1, key1, grain2, key2), list of registered update references
    {
        { SnapperKeyReferenceType.ReplicateReference, new Dictionary<GrainID, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>() },
        { SnapperKeyReferenceType.DeleteReference, new Dictionary<GrainID, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>() }
    };

    foreach (var item in masterGrains)
    {
        var grainID = item.Key;
        var dictionary = SnapperSerializer.DeserializeDictionary(item.Value.Item1);
        var updateReferences = SnapperSerializer.DeserializeReferences(item.Value.Item2);
        var deleteReferences = SnapperSerializer.DeserializeReferences(item.Value.Item3);
        var list = SnapperSerializer.DeserializeUpdatesOnList(item.Value.Item4);

        var copiedUpdateReferences = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        foreach (var aitem in updateReferences)
        {
            copiedUpdateReferences.Add(aitem.Key, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());
            foreach (var bitem in aitem.Value)
            {
                copiedUpdateReferences[aitem.Key].Add(bitem.Key, new Dictionary<ISnapperKey, IUpdateFunction>());
                foreach (var citem in bitem.Value) copiedUpdateReferences[aitem.Key][bitem.Key].Add(citem.Key, citem.Value);
            }
        }

        var copiedDeleteReferences = new Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>();
        foreach (var aitem in deleteReferences)
        {
            copiedDeleteReferences.Add(aitem.Key, new Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>());
            foreach (var bitem in aitem.Value)
            {
                copiedDeleteReferences[aitem.Key].Add(bitem.Key, new Dictionary<ISnapperKey, IUpdateFunction>());
                foreach (var citem in bitem.Value) copiedDeleteReferences[aitem.Key][bitem.Key].Add(citem.Key, citem.Value);
            }
        }

        var references = new Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>  // (type, grain1, key1, grain2, key2), list of registered update references
        {
            { SnapperKeyReferenceType.ReplicateReference, copiedUpdateReferences },
            { SnapperKeyReferenceType.DeleteReference, copiedDeleteReferences }
        };

        newMasterGrains.Add(grainID, new SnapperGrainState(grainID, dictionary, references, list));

        Debug.Assert(!masterKeyInfo.ContainsKey(grainID));
        masterKeyInfo.Add(grainID, new Dictionary<ISnapperKey, SnapperValue>());
        foreach (var kv in dictionary)
        {
            var key = kv.Key;
            var value = kv.Value;

            switch (value.referenceType)
            {
                case SnapperKeyReferenceType.NoReference:
                    Debug.Assert(value.dependentGrain == null && value.dependentKey == null);
                    Debug.Assert(!masterKeyInfo[grainID].ContainsKey(key));
                    masterKeyInfo[grainID].Add(key, value);
                    break;
                case SnapperKeyReferenceType.ReplicateReference:
                case SnapperKeyReferenceType.DeleteReference:
                    followerKeyInfo.Add((grainID, key, value));
                    break;
                default:
                    continue;
            }
        }

        referenceInfo[SnapperKeyReferenceType.ReplicateReference].Add(grainID, updateReferences);
        referenceInfo[SnapperKeyReferenceType.DeleteReference].Add(grainID, deleteReferences);
    }

    // check in master grains, if keys with update reference are consistent with the original key
    var numUpdateReferences = 0;
    var numDeleteReferences = 0;
    foreach (var item in followerKeyInfo)
    {
        var grain2 = item.Item1;
        var key2 = item.Item2;
        var value2 = item.Item3;

        var referenceType = value2.referenceType;
        Debug.Assert(referenceType != SnapperKeyReferenceType.NoReference);

        var dependentGrain = value2.dependentGrain;
        var dependentKey = value2.dependentKey;
        Debug.Assert(dependentGrain != null && dependentKey != null);
        if (!masterKeyInfo.ContainsKey(dependentGrain))
            Console.WriteLine($"the grain {dependentGrain.id}, {dependentGrain.className} does not exist");
        if (!masterKeyInfo[dependentGrain].ContainsKey(dependentKey))
        {
            Console.WriteLine($"Grain {grain2.Print()}: find key with reference, key = {key2.Print()}");
            Console.WriteLine($"Grain {dependentGrain.Print()}: key {dependentKey.Print()} does not exists. ");
            Console.WriteLine($"Grain {dependentGrain.Print()}: master keys = {string.Join(", ", masterKeyInfo[dependentGrain].Keys.Select(x => x.Print()))}");
        }
        var value1 = masterKeyInfo[dependentGrain][dependentKey];

        // check if the reference info is registered
        Debug.Assert(referenceInfo[referenceType].ContainsKey(dependentGrain));
        if (!referenceInfo[referenceType][dependentGrain].ContainsKey(dependentKey))
        {
            Console.WriteLine($"Grain {dependentGrain.Print()}: try to find master key {dependentKey.Print()}");
            Console.WriteLine($"Grain {dependentGrain.Print()}: master keys = {string.Join(", ", referenceInfo[referenceType][dependentGrain].Keys.Select(x => x.Print()))}");
        }
        Debug.Assert(referenceInfo[referenceType][dependentGrain].ContainsKey(dependentKey));
        Debug.Assert(referenceInfo[referenceType][dependentGrain][dependentKey].ContainsKey(grain2));
        Debug.Assert(referenceInfo[referenceType][dependentGrain][dependentKey][grain2].ContainsKey(key2));
        referenceInfo[referenceType][dependentGrain][dependentKey][grain2].Remove(key2);

        if (referenceType == SnapperKeyReferenceType.ReplicateReference)
        {
            // check if value2 equals to value1
            Debug.Assert(key2.Equals(dependentKey));
            Debug.Assert(value2.keyTypeName == value1.keyTypeName);

            if (!value2.value.Equals(value1.value))
            {
                Console.WriteLine($"Grain {dependentGrain.Print()}: origin value = {value2.value.GetAssemblyQualifiedName()}, {value2.value.Print()}");
                Console.WriteLine($"Grain {grain2.Print()}: copied value = {value1.value.GetAssemblyQualifiedName()}, {value1.value.Print()}");
            }
            Debug.Assert(value2.value.Equals(value1.value));
            numUpdateReferences++;
        }
        else numDeleteReferences++;
    }

    // check if all registered references have the corresponding followers existing
    foreach (var i1 in referenceInfo) foreach (var i2 in i1.Value) foreach (var i3 in i2.Value) foreach (var i4 in i3.Value) Debug.Assert(i4.Value.Count == 0);

    Console.WriteLine($"CheckDataConsistency: {followerKeyInfo.Count} follower keys match {numUpdateReferences} update reference and {numDeleteReferences} delete reference info. ");
    Console.WriteLine($"CheckDataConsistency: {followerKeyInfo.Count} follower keys are consistent with origin keys. ");

    foreach (var item in replicaGrains)
    {
        var grainID = item.Item1;
        var dictionary = SnapperSerializer.DeserializeDictionary(item.Item2.Item1);
        var updateReferences = SnapperSerializer.DeserializeReferences(item.Item2.Item2);
        var deleteReferences = SnapperSerializer.DeserializeReferences(item.Item2.Item3);
        var list = SnapperSerializer.DeserializeUpdatesOnList(item.Item2.Item4);

        var references = new Dictionary<SnapperKeyReferenceType, Dictionary<ISnapperKey, Dictionary<GrainID, Dictionary<ISnapperKey, IUpdateFunction>>>>  // (type, grain1, key1, grain2, key2), list of registered update references
        {
            { SnapperKeyReferenceType.ReplicateReference, updateReferences },
            { SnapperKeyReferenceType.DeleteReference, deleteReferences }
        };

        newReplicaGrains.Add((grainID, new SnapperGrainState(grainID, dictionary, references, list)));
    }

    // check if replicas are consistent with the master
    IGrainNameHelper grainNameHelper;
    switch (basicEnvSetting.benchmark)
    {
        case BenchmarkType.SMALLBANK:
            grainNameHelper = new SmallBank.Workload.GrainNameHelper();
            break;
        case BenchmarkType.MARKETPLACE:
            grainNameHelper = new MarketPlace.Workload.GrainNameHelper();
            break;
        default:
            throw new Exception($"The {basicEnvSetting.benchmark} benchmark is not supported. ");
    }

    foreach (var item in newReplicaGrains)
    {
        var replicaGrainID = item.Item1;
        var masterGrainID = grainNameHelper.GetMasterGrainID(basicEnvSetting.implementationType, replicaGrainID);
        Debug.Assert(newMasterGrains.ContainsKey(masterGrainID));

        // check if dictionary state is the same
        var master = newMasterGrains[masterGrainID];
        Debug.Assert(master.dictionary.Count == item.Item2.dictionary.Count);
        foreach (var kv in item.Item2.dictionary)
        {
            // check if each kv pair is the same
            Debug.Assert(master.dictionary.ContainsKey(kv.Key));
            var masterValue = master.dictionary[kv.Key];
            var replicaValue = kv.Value;
            Debug.Assert(masterValue.value.Equals(replicaValue.value));
            Debug.Assert(masterValue.keyTypeName.Equals(replicaValue.keyTypeName));
            var masterValueTypeName = masterValue.value.GetAssemblyQualifiedName();
            Debug.Assert(!string.IsNullOrEmpty(masterValueTypeName));
            Debug.Assert(masterValueTypeName.Equals(replicaValue.value.GetAssemblyQualifiedName()));
            Debug.Assert(masterValue.referenceType.Equals(replicaValue.referenceType));
            if (masterValue.dependentGrain == null)
            {
                Debug.Assert(replicaValue.dependentGrain == null);
                Debug.Assert(masterValue.dependentKey == null);
                Debug.Assert(replicaValue.dependentKey == null);
            }
            else
            {
                Debug.Assert(replicaValue.dependentGrain != null);
                Debug.Assert(masterValue.dependentKey != null);
                Debug.Assert(replicaValue.dependentKey != null);
                Debug.Assert(masterValue.dependentGrain.Equals(replicaValue.dependentGrain));
                Debug.Assert(masterValue.dependentKey.Equals(replicaValue.dependentKey));
            }
        }

        // check if reference info is the same
        Debug.Assert(master.references.Count == item.Item2.references.Count);
        foreach (var info in item.Item2.references)
        {
            var referenceType = info.Key;
            Debug.Assert(master.references.ContainsKey(referenceType));
            Debug.Assert(info.Value.Count == master.references[referenceType].Count);
            foreach (var keyInfo in info.Value)
            {
                var key = keyInfo.Key;
                Debug.Assert(master.references[referenceType].ContainsKey(key));
                Debug.Assert(keyInfo.Value.Count == master.references[referenceType][key].Count);
                foreach (var followerInfo in keyInfo.Value)
                {
                    var followerGrain = followerInfo.Key;
                    Debug.Assert(master.references[referenceType][key].ContainsKey(followerGrain));
                    /*
                    if (followerInfo.Value.Count != master.references[referenceType][key][followerGrain].Count)
                    {
                        Console.WriteLine($"referenceType = {referenceType}, key = {key.Print()}, followerGrain = {followerGrain.Print()}");
                        Console.WriteLine();
                        Console.WriteLine();
                        Console.WriteLine();
                        foreach (var k in followerInfo.Value) Console.WriteLine($"replica contains key {k.Key.Print()}");
                        Console.WriteLine();
                        Console.WriteLine();
                        Console.WriteLine();
                        foreach (var k in master.references[referenceType][key][followerGrain]) Console.WriteLine($"master contains key {k.Key.Print()}");
                    }
                    */
                    Debug.Assert(followerInfo.Value.Count == master.references[referenceType][key][followerGrain].Count);
                    foreach (var followerKey in followerInfo.Value) Debug.Assert(master.references[referenceType][key][followerGrain].ContainsKey(followerKey.Key));
                }
            }
        }

        // check if each log record is the same
        Debug.Assert(master.list.Count == item.Item2.list.Count);
        for (var i = 0; i < master.list.Count; i++)
        {
            var masterRecord = master.list[i];
            var replicaRecord = item.Item2.list[i];
            Debug.Assert(masterRecord.record.Equals(replicaRecord.record));
            Debug.Assert(masterRecord.recordTypeName.Equals(replicaRecord.recordTypeName));
        }
    }

    Console.WriteLine($"CheckDataConsistency: {replicaGrains.Count} replica grains have the same state as {masterGrains.Count} master grains");
}
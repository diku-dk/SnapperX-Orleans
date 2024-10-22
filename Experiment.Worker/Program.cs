using NetMQ;
using Utilities;
using NetMQ.Sockets;
using System.Diagnostics;
using MessagePack;
using Experiment.Worker;
using StackExchange.Redis;
using Concurrency.Common.Cache;
using Experiment.Common;

// =========================================================================================================================
// parse all input arguments
var isLocalTest = bool.Parse(args[0]);
var myRole = Enum.Parse<SnapperRoleType>(args[1]);
var myRegion = args[2];
var controller_PublicIP = args[3];
var myID = isLocalTest ? "+" + args[4] : "";   // this is only used when doing local test

// =========================================================================================================================
// set up socket connections to controller
string prefix;
if (isLocalTest) prefix = ">tcp://localhost:";
else prefix = ">tcp://" + controller_PublicIP + ":";
var pushSocket = new PushSocket(prefix + Constants.controllerPullPort);
var subscribeSocket = new SubscriberSocket(prefix + Constants.controllerPublishPort);

// =========================================================================================================================
subscribeSocket.Subscribe(NetMsgType.ENV_SETTING.ToString());
var myPublicIP = Helper.GetPublicIPAddress() + myID;
var msgContent = MessagePackSerializer.Serialize((myRole, myRegion, myPublicIP));
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.CONNECT, msgContent)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.CONNECT}, {myRole}, {myRegion}, {myPublicIP}");

// =========================================================================================================================
subscribeSocket.ReceiveFrameString();    // this is the topic name
var msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.ENV_SETTING);
subscribeSocket.Unsubscribe(NetMsgType.ENV_SETTING.ToString());
(var benchmark, var bytes) = MessagePackSerializer.Deserialize<(BenchmarkType, byte[])>(msg.content);

IEnvSetting envSetting;
switch (benchmark)
{
    case BenchmarkType.SMALLBANK:
        envSetting = MessagePackSerializer.Deserialize<SmallBank.Workload.EnvSetting>(bytes);
        break;
    case BenchmarkType.MARKETPLACE:
        envSetting = MessagePackSerializer.Deserialize<MarketPlace.Workload.EnvSetting>(bytes);
        break;
    default:
        throw new Exception($"The benchmark {benchmark} is not supported. ");
}
var basicEnvSetting = envSetting.GetBasic();

var regionIndex = basicEnvSetting.roleInfo[myPublicIP].Item1;
var siloIndex = basicEnvSetting.roleInfo[myPublicIP].Item2;
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.ENV_SETTING}, regionIndex = {regionIndex}, siloIndex = {siloIndex}");

// =========================================================================================================================
subscribeSocket.Subscribe(NetMsgType.ENV_INIT.ToString());
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

// =========================================================================================================================
subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.ENV_INIT);
var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { basicEnvSetting.redis_ConnectionString } });
var clusterInfo_db = redis.GetDatabase(Constants.Redis_ClusterInfo);
var staticClusterInfo = MessagePackSerializer.Deserialize<StaticClusterInfo>(clusterInfo_db.StringGet("clusterInfo"));
var mySilo = staticClusterInfo.localSiloListPerRegion[myRegion][siloIndex];

StaticReplicaInfo? staticReplicaInfo = null;
string myReplicaSiloID = "";
if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication)
{
    var replicaInfo_db = redis.GetDatabase(Constants.Redis_ReplicaInfo);
    staticReplicaInfo = MessagePackSerializer.Deserialize<StaticReplicaInfo>(replicaInfo_db.StringGet("replicaInfo"));
    var replicaSiloList = staticReplicaInfo.localReplicaSiloListPerRegion[myRegion];
    myReplicaSiloID = replicaSiloList[siloIndex % replicaSiloList.Count];
    Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.ENV_INIT}, cluster and replica info is in Redis");
}

var envConfigureInfo_db = redis.GetDatabase(Constants.Redis_EnvConfigureInfo);
var serializedEnvConfigureInfo = await envConfigureInfo_db.StringGetAsync("envConfigureInfo");
IEnvConfigure envConfigureHelper;
switch (benchmark)
{
    case BenchmarkType.SMALLBANK:
        envConfigureHelper = MessagePackSerializer.Deserialize<SmallBank.Workload.EnvConfigure>(serializedEnvConfigureInfo);
        break;
    case BenchmarkType.MARKETPLACE:
        envConfigureHelper = MessagePackSerializer.Deserialize<MarketPlace.Workload.EnvConfigure>(serializedEnvConfigureInfo);
        break;
    default:
        throw new Exception($"The benchmark {benchmark} is not supported. ");
}

// =========================================================================================================================
subscribeSocket.Subscribe(NetMsgType.WORKLOAD_INIT.ToString());
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

// =========================================================================================================================
subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.WORKLOAD_INIT);
var numExperiment = MessagePackSerializer.Deserialize<int>(msg.content);
(var numEpoch, var numReRun) = basicEnvSetting.GetNumEpochAndNumReRun();
var maxNumExperiment = numReRun * numExperiment;
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.WORKLOAD_INIT}, numExperiment = {numExperiment}");

// =========================================================================================================================
subscribeSocket.Unsubscribe(NetMsgType.WORKLOAD_INIT.ToString());
subscribeSocket.Subscribe(NetMsgType.WORKLOAD_CONFIG.ToString());
subscribeSocket.Subscribe(NetMsgType.TERMINATE.ToString());
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

// =========================================================================================================================
// Set up processor affinity
if (!isLocalTest) Helper.SetCPU("Experiment.Worker", basicEnvSetting.roleSizes[myRole]);

// =========================================================================================================================
// run experiment one by one
var clients = new List<IClusterClient>();
for (int experimentID = 0; experimentID <= maxNumExperiment; experimentID++)
{
    subscribeSocket.ReceiveFrameString();   // this is the topic name
    msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
    if (msg.msgType == NetMsgType.TERMINATE)
    {
        Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.TERMINATE}");
        break;
    }

    Debug.Assert(msg.msgType == NetMsgType.WORKLOAD_CONFIG);

    Console.WriteLine($"{myRole}: start experiment {experimentID} / (num = {numExperiment}, max = {maxNumExperiment})");
    IWorkloadConfigure w1;
    IWorkloadConfigure? w2 = null;
    (var bytes1, var bytes2) = MessagePackSerializer.Deserialize<(byte[], byte[])>(msg.content);
    switch (benchmark)
    {
        case BenchmarkType.SMALLBANK:
            w1 = MessagePackSerializer.Deserialize<SmallBank.Workload.WorkloadConfigure>(bytes1);
            if (bytes2.Length != 0) w2 = MessagePackSerializer.Deserialize<SmallBank.Workload.WorkloadConfigure>(bytes2);
            break;
        case BenchmarkType.MARKETPLACE:
            w1 = MessagePackSerializer.Deserialize<MarketPlace.Workload.WorkloadConfigure>(bytes1);
            if (bytes2.Length != 0) w2 = MessagePackSerializer.Deserialize<MarketPlace.Workload.WorkloadConfigure>(bytes2);
            break;
        default:
            throw new Exception($"{myRole}: the benchmark {benchmark} is not supported. ");
    }
    Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.WORKLOAD_CONFIG}, workload and replica workload");

    SingleExperimentManager singleExperimentManager;
    switch (myRole)
    {
        case SnapperRoleType.Worker:
            singleExperimentManager = new SingleExperimentManager(false, myRegion, mySilo, myReplicaSiloID, regionIndex, siloIndex, envSetting, envConfigureHelper, w1, clients, staticClusterInfo, staticReplicaInfo);
            break;
        case SnapperRoleType.ReplicaWorker:
            Debug.Assert(w2 != null);
            singleExperimentManager = new SingleExperimentManager(true, myRegion, mySilo, myReplicaSiloID, regionIndex, siloIndex, envSetting, envConfigureHelper, w2, clients, staticClusterInfo, staticReplicaInfo);
            break;
        default: throw new Exception("This should never happen");
    }

    // initialize threads and other data structures for epoch runs
    Console.WriteLine($"{myRole}: finish initialization");

    subscribeSocket.Subscribe("RUN_EPOCH");
    pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
    Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

    for (int i = 0; i < numEpoch; i++)
    {
        subscribeSocket.ReceiveFrameString();
        msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
        Debug.Assert(msg.msgType == NetMsgType.RUN_EPOCH);
        Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.RUN_EPOCH}, {i}");

        var results = singleExperimentManager.RunEpoch(i);
        var result = ExperimentResultAggregator.AggregateResultForEpoch(results);
        msg = new NetworkMessage(NetMsgType.ACK, MessagePackSerializer.Serialize((myRole, result)));
        pushSocket.SendFrame(MessagePackSerializer.Serialize(msg));
        Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}, workloadResult");
    }

    subscribeSocket.Unsubscribe("RUN_EPOCH");
    Console.WriteLine($"{myRole}: finish experiment {experimentID}");
}
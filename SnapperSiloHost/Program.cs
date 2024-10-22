using NetMQ;
using Utilities;
using System.Net;
using Orleans.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Concurrency.Implementation.GrainPlacement;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using NetMQ.Sockets;
using MessagePack;
using System.Diagnostics;
using Amazon.DynamoDBv2;
using SnapperSiloHost;
using Concurrency.Interface;
using Concurrency.Implementation;
using Amazon.DynamoDBv2.Model;
using Concurrency.Common.Logging;
using Concurrency.Common.ILogging;
using Replication.Implementation.GrainReplicaPlacement;
using Replication.Interface;
using Replication.Implementation;
using Concurrency.Common.ICache;
using Concurrency.Common.Cache;
using Experiment.Common;
using SnapperSiloHost.OrleansStorage;
using Concurrency.Common;

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
NetMsgType targetMsg;
switch (myRole)
{
    case SnapperRoleType.GlobalSilo:
        targetMsg = NetMsgType.START_GLOBAL_SILO;
        break;
    case SnapperRoleType.RegionalSilo:
        targetMsg = NetMsgType.START_REGIONAL_SILO;
        break;
    default:
        targetMsg = NetMsgType.START_OTHER_SILO;
        break;
}

subscribeSocket.Subscribe(targetMsg.ToString());
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == targetMsg);
subscribeSocket.Unsubscribe(targetMsg.ToString());
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {targetMsg}");

// =========================================================================================================================
// decide which port to use if all silos are on one machine
var siloPort = 11111;      // silo-to-silo endpoint
var gatewayPort = 30000;   // client-to-silo endpoint
var localSiloIndex = Helper.GetLocalSiloIndex(basicEnvSetting.numSiloPerRegion, regionIndex, siloIndex);
if (isLocalTest)
{
    switch (myRole)
    {
        case SnapperRoleType.RegionalSilo:
            siloPort += 1 + regionIndex;
            gatewayPort += 1 + regionIndex;
            break;
        case SnapperRoleType.LocalSilo:
            siloPort += 1 + basicEnvSetting.numRegion + localSiloIndex;
            gatewayPort += 1 + basicEnvSetting.numRegion + localSiloIndex;
            break;
        case SnapperRoleType.LocalReplicaSilo:
            siloPort += 1 + basicEnvSetting.numRegion + basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion + localSiloIndex;
            gatewayPort += 1 + basicEnvSetting.numRegion + basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion + localSiloIndex;
            break;
    }
}

// =========================================================================================================================
// configure and start silo
(var siloHost, var siloAddress) = await StartSilo();

switch (myRole)
{
    case SnapperRoleType.LocalReplicaSilo:
        targetMsg = NetMsgType.INIT_REPLICA_SILO;
        break;
    case SnapperRoleType.LocalSilo:
        targetMsg = NetMsgType.INIT_LOCAL_SILO;
        break;
     default:
        targetMsg = NetMsgType.ENV_INIT;
        break;
}

subscribeSocket.Subscribe(targetMsg.ToString());
msgContent = MessagePackSerializer.Serialize((myPublicIP, siloAddress));
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK, msgContent)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}, {siloAddress}");

// =========================================================================================================================
// wait for controller to init cluster info
subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == targetMsg);
subscribeSocket.Unsubscribe(targetMsg.ToString());
var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { basicEnvSetting.redis_ConnectionString } });

var clusterInfo_db = redis.GetDatabase(Constants.Redis_ClusterInfo);
var staticClusterInfo = MessagePackSerializer.Deserialize<StaticClusterInfo>(clusterInfo_db.StringGet("clusterInfo"));

StaticReplicaInfo? staticReplicaInfo = null;
if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication)
{
    var replicaInfo_db = redis.GetDatabase(Constants.Redis_ReplicaInfo);
    staticReplicaInfo = MessagePackSerializer.Deserialize<StaticReplicaInfo>(replicaInfo_db.StringGet("replicaInfo"));
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
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {targetMsg}");

// =========================================================================================================================
// init the cluster cache info
var snapperClusterCache = siloHost.Services.GetRequiredService<ISnapperClusterCache>();
(var grainIDToPartitionID, var partitionIDToMasterInfo, var grainIDToMigrationWorker) = envConfigureHelper.GetGrainPlacementInfo();
snapperClusterCache.PrepareCache(staticClusterInfo, grainIDToPartitionID, partitionIDToMasterInfo, grainIDToMigrationWorker);
Console.WriteLine($"{myRole}: cluster cache is prepared, it contains {grainIDToPartitionID.Count} entries in grainIDToPartitionID");

// =========================================================================================================================
TransactionExecutionSilo? transactionExecutionSilo = null;
if (myRole != SnapperRoleType.LocalReplicaSilo)
{
    transactionExecutionSilo = new TransactionExecutionSilo(myRole, myRegion, siloAddress, envSetting, envConfigureHelper, siloHost, staticReplicaInfo);
    await transactionExecutionSilo.Init();
}

// =========================================================================================================================
TransactionReplicationSilo? transactionReplicationSilo = null;
if (myRole == SnapperRoleType.RegionalSilo || myRole == SnapperRoleType.LocalReplicaSilo)
{
    if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication)
    {
        Debug.Assert(staticReplicaInfo != null);
        transactionReplicationSilo = new TransactionReplicationSilo(myRole, myRegion, siloAddress, envSetting, siloHost, staticReplicaInfo, envConfigureHelper);
        await transactionReplicationSilo.Init();
    }
}

// =========================================================================================================================
// Set up processor affinity
if (!isLocalTest) Helper.SetCPU("SnapperSiloHost", basicEnvSetting.roleSizes[myRole]);

// =========================================================================================================================
subscribeSocket.Subscribe(NetMsgType.WORKLOAD_INIT.ToString());
msgContent = MessagePackSerializer.Serialize(siloAddress);
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK, msgContent)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

// =========================================================================================================================
subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.WORKLOAD_INIT);
subscribeSocket.Unsubscribe(NetMsgType.WORKLOAD_INIT.ToString());
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.WORKLOAD_INIT}");
await CheckGC();

// =========================================================================================================================
subscribeSocket.Subscribe(NetMsgType.FINISH_EXP.ToString());
msgContent = MessagePackSerializer.Serialize(siloAddress);
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK, msgContent)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

// =========================================================================================================================
Console.WriteLine($"{myRole}: waits for experiments to finish");
Console.WriteLine($"========================================================================================================");
Console.WriteLine();
subscribeSocket.Subscribe(NetMsgType.TERMINATE.ToString());
subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.FINISH_EXP);
subscribeSocket.Unsubscribe(NetMsgType.FINISH_EXP.ToString());
Console.WriteLine($"========================================================================================================");
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.FINISH_EXP}");
var aggregatedExperimentData = await CheckGC();

// =========================================================================================================================
// get the current state of each grain
var state = new byte[0];
/*
if (basicEnvSetting.benchmark == BenchmarkType.MARKETPLACE)
{
    switch (myRole)
    {
        case SnapperRoleType.LocalSilo:
            Debug.Assert(transactionExecutionSilo != null);
            Console.WriteLine($"Try to get all grain state...");
            var data = await transactionExecutionSilo.GetAllGrainState();
            state = MessagePackSerializer.Serialize(data);
            Console.WriteLine($"{myRole}: get state of {data.Count} grains");
            break;
        case SnapperRoleType.LocalReplicaSilo:
            Debug.Assert(transactionReplicationSilo != null);
            Console.WriteLine($"Try to get all replica grain state...");
            var replicaData = await transactionReplicationSilo.GetAllGrainState();
            state = MessagePackSerializer.Serialize(replicaData);
            Console.WriteLine($"{myRole}: get state of {replicaData.Count} replica grains");
            break;
    }
}
*/

// =========================================================================================================================
subscribeSocket.Subscribe(NetMsgType.TERMINATE.ToString());
msgContent = MessagePackSerializer.Serialize((myRole, state, aggregatedExperimentData));
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK, msgContent)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");

// =========================================================================================================================
Console.WriteLine($"{myRole}: waits for terminate message");
subscribeSocket.ReceiveFrameString();    // this is the topic name
msg = MessagePackSerializer.Deserialize<NetworkMessage>(subscribeSocket.ReceiveFrameBytes());
Debug.Assert(msg.msgType == NetMsgType.TERMINATE);
subscribeSocket.Unsubscribe(NetMsgType.TERMINATE.ToString());
Console.WriteLine($"{myRole} <== {SnapperRoleType.Controller}: {NetMsgType.TERMINATE}");

// =========================================================================================================================
// terminate silo
pushSocket.SendFrame(MessagePackSerializer.Serialize(new NetworkMessage(NetMsgType.ACK)));
Console.WriteLine($"{myRole} ==> {SnapperRoleType.Controller}: {NetMsgType.ACK}");
_ = siloHost.StopAsync();

async Task<(IHost, string)> StartSilo()
{
    // =========================================================================================================================
    // build silo host
    Console.WriteLine($"{myRole}: ready to start silo");
    var siloHost = new HostBuilder().UseOrleans(CreateSilo).Build();

    var i = 0;
    var maxAttempt = 5;
    for (i = 0; i < maxAttempt; i++)
    {
        try
        {
            await siloHost.StartAsync();
            Console.WriteLine($"{myRole}: silo is started");
            break;
        }
        catch (ResourceNotFoundException)
        {
            Console.WriteLine($"{myRole}: attempt {i} failed to start silo. ");
            var time = new Random().Next(0, 10);
            Thread.Sleep(TimeSpan.FromSeconds(time));
        }
    }
    if (i == maxAttempt) throw new Exception($"{myRole}: fail to start silo");
    
    // =========================================================================================================================
    // get SiloAddress by reading the dynamoDB table
    Console.WriteLine($"{myRole}: retrieve siloAddress from DynamoDB");
    var tableName = Helper.GetMembershipTableName(myRegion);
    var attributesToGet = new List<string> { "SiloIdentity", "SiloStatus" };
    var dynamoDBClient = new AmazonDynamoDBClient(basicEnvSetting.accessKey, basicEnvSetting.secretKey, Helper.ParseRegionEndpoint(myRegion));
    var items = (await dynamoDBClient.ScanAsync(tableName, attributesToGet)).Items;
    string siloAddress = "";
    var myPrivateIP = Helper.GetLocalIPAddress();
    foreach (var item in items)
    {
        var silo = item["SiloIdentity"].S;   // string
        var status = item["SiloStatus"].N;   // number
        if (status != "3") continue;         // "3" means "active"

        // RuntimeIdentity format: S127.0.0.1:11112:396016232
        // SiloIdentity format:     127.0.0.1-11112-396016232
        if (silo.Contains(myPrivateIP) && silo.Contains(siloPort.ToString()))
        {
            siloAddress = "S" + silo.Replace('-', ':');
            break;
        }
    }
    Debug.Assert(siloAddress.Length != 0);
    return (siloHost, siloAddress);
}

void CreateSilo(ISiloBuilder builder)
{
    // start the silo
    Action<DynamoDBClusteringOptions> dynamoDBOptions = options =>
    {
        options.AccessKey = basicEnvSetting.accessKey;
        options.SecretKey = basicEnvSetting.secretKey;
        options.TableName = Helper.GetMembershipTableName(myRegion);
        options.Service = myRegion;
    };

    builder
        .UseDynamoDBClustering(dynamoDBOptions)
        .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(Helper.GetLocalIPAddress()));

    builder
        .Configure<ClusterOptions>(options =>
        {
            options.ClusterId = myRegion;
            options.ServiceId = myRegion;
        })
        .Configure<EndpointOptions>(options =>
        {
            options.SiloPort = siloPort;
            options.GatewayPort = gatewayPort;
            Console.WriteLine($"{myRole}: siloPort = {siloPort}, gatewayPort = {gatewayPort}");
        })
        .ConfigureServices(ConfigureServices)
        .AddPlacementDirector<SnapperGrainPlacementStrategy, SnapperGrainPlacement>()
        .AddPlacementDirector<SnapperReplicaGrainPlacementStrategy, SnapperReplicaGrainPlacement>()
        .AddRedisGrainDirectory(Constants.GrainDirectoryName, options =>
        {
            options.ConfigurationOptions = new ConfigurationOptions
            {
                EndPoints = { basicEnvSetting.redis_ConnectionString },
                DefaultDatabase = Constants.Redis_GrainDirectory
            };
        })
        .AddMemoryStreams(Constants.DefaultStreamProvider, options =>
        {
            options.ConfigureStreamPubSub(Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit);
        })
        .AddMemoryGrainStorage(Constants.DefaultStreamStorage)
        .UseDashboard(options => { });
        //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

    if (basicEnvSetting.implementationType == ImplementationType.ORLEANSTXN)
    {
        try
        {
            builder.UseTransactions();

            if (!basicEnvSetting.doLogging)
            {
                builder.AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });
            }
            else
            {
                builder.AddFileTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; opts.siloID = siloIndex; opts.numPartitionPerSilo = envSetting.GetNumPartitionPerSilo(); opts.regionID = regionIndex; });

                //builder
                //.Configure<TransactionalStateOptions>(o => o.LockTimeout = TimeSpan.FromMilliseconds(200))
                //.Configure<TransactionalStateOptions>(o => o.LockAcquireTimeout = TimeSpan.FromMilliseconds(200));
                //.Configure<TransactionalStateOptions>(o => o.PrepareTimeout = TimeSpan.FromSeconds(20));
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"{e.Message} {e.StackTrace}");
            throw;
        }
    }
    else builder.AddMemoryGrainStorageAsDefault();

    Console.WriteLine($"{myRole}: creating silo...");
}

void ConfigureServices(IServiceCollection services)
{
    // all the singletons have one instance per silo host
    // dependency injection <TService, TImplementation>
    services.AddSingleton<ISnapperClusterCache, SnapperClusterCache>();
    services.AddSingleton<ISnapperReplicaCache, SnapperReplicaCache>();

    var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { basicEnvSetting.redis_ConnectionString } });
    services.AddSingleton<IConnectionMultiplexer>(redis);

    var redisServer = redis.GetServer(basicEnvSetting.redis_ConnectionString);
    services.AddSingleton(redisServer);

    if (basicEnvSetting.implementationType == ImplementationType.SNAPPER || 
        basicEnvSetting.implementationType == ImplementationType.SNAPPERSIMPLE || 
        basicEnvSetting.implementationType == ImplementationType.SNAPPERFINE ||
        basicEnvSetting.implementationType == ImplementationType.NONTXNKV)
    {
        if (myRole == SnapperRoleType.LocalReplicaSilo || myRole == SnapperRoleType.RegionalSilo)
        {
            if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication)
            {
                services.AddSingleton<IHistoryManager, HistoryManager>();
            }
        }

        if (myRole == SnapperRoleType.LocalReplicaSilo) services.AddSingleton<ISnapperSubscriber, SnapperSubscriber>();
        else
        {
            services.AddSingleton<IScheduleManager, ScheduleManager>();
            services.AddSingleton<ISnapperLoggingHelper, SnapperLoggingHelper>();
        }
    }
    else
    { 
        if (myRole != SnapperRoleType.LocalReplicaSilo) services.AddSingleton<ISnapperLogger, SnapperLogger>();
    }
}

async Task<AggregatedExperimentData> CheckGC()
{
    var data = new AggregatedExperimentData();

    try
    {
        if (transactionExecutionSilo != null) data = await transactionExecutionSilo.CheckGC();
    }
    catch (Exception e)
    {
        Console.WriteLine($"{e.Message}, {e.StackTrace}");
        Debug.Assert(false);
    }

    var count = 0;
    if (transactionReplicationSilo != null)
    {
        while (count < Constants.maxNumReRun)
        {
            Console.WriteLine($"{myRole}: wait for 10s and check GC");
            await Task.Delay(TimeSpan.FromSeconds(10));

            if (await transactionReplicationSilo.CheckGC())
            {
                Console.WriteLine($"{myRole}: everything is cleaned up, count = {count}");
                count++;
            }
            else count = 0;
        }
    }

    return data;
}
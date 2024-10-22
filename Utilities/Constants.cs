namespace Utilities;

public enum AccessMode { Read, ReadWrite, NoOp };
public enum Hierarchy { Global, Regional, Local };
public enum ExceptionType { RWConflict, NotSerializable, AppAbort, GrainMigration };
public enum BreakDownLatency { 
    REGISTER,          // grian receive txn request => get transaction context
    GETSCHEDULE,       // ... => the first grain get the schedule registered no the grain
    GETTURN,           // ... => get the turn to start acquire locks on the grain
    GETSTATE,          // ... => get access to the acquired grain state
    EXECUTE,           // ... => the first accessed grain finishes executing the whole transaction
    PREPARE,           // ... => 
    COMMIT };          // ... => transaction is committed on the first grain
public enum BenchmarkType { SMALLBANK, MARKETPLACE };
public enum ImplementationType { 
    NONTXN,            // no data model + no transaction
    NONTXNKV,          // kv data model + no transaction
    ORLEANSTXN,        // no data model + orleans transaction
    SNAPPERSIMPLE,     // no data model (one kv pair per actor) + snapper transaction
    SNAPPER,           // data model + actor-level concurrency control
    SNAPPERFINE };     // data model + key-level concurrency control
public enum ConsumerThreadType { PACT, ACT, GrainMigration };
public enum GrainMigrationTimeInterval { FreezeGrain, UpdateRedis, UpdateCache, DeactivateGrain, ActivateGrain, UnFreezeGrain };
public enum SnapperLogType { Info, Prepare, Commit };
public enum SnapperRoleType { Controller, Worker, ReplicaWorker, GlobalSilo, RegionalSilo, LocalSilo, LocalReplicaSilo };
public enum SnapperInternalFunction { ReadState, NoOp,
    RegisterReference, DeRegisterReference, ApplyForwardedKeyOperations,
    RegisterFineReference, DeRegisterFineReference, ApplyForwardedFineKeyOperations};

/// <summary>
/// delete reference is the most basic, other types of reference implicitly include delete reference 
/// replicate reference refers to replica keeps consistent with the primary copy (replicate new value of the key directly)
/// update reference refers to the update on a key will trigger an update function happened on another key (a more complicated version of replicate reference)
/// </summary>
public enum SnapperKeyReferenceType { NoReference, DeleteReference, ReplicateReference, UpdateReference };

public enum SmallBankTxnType { Init, MultiTransfer, CollectBalance, ReadState };
public enum TestAppTxnType { Init, DoOp, ReadState, RegisterUpdateReference, DeRegisterUpdateReference, RegisterDeleteReference, DeRegisterDeleteReference }
public enum MarketPlaceTxnType { 
    AddItemToCart, DeleteItemInCart, Checkout,  // entry point: cart actor
    UpdatePrice, DeleteProduct, AddProducts,    // entry point: product actor
    AddStock, ReplenishStock,                   // entry point: stock actor
    RefreshSellerDashboard };                   // entry point: seller actor   

public enum NetMsgType { CONNECT, ENV_SETTING, 
    START_GLOBAL_SILO, START_REGIONAL_SILO, START_OTHER_SILO,
    ENV_INIT, INIT_REPLICA_SILO, INIT_LOCAL_SILO,
    WORKLOAD_INIT, WORKLOAD_CONFIG, RUN_EPOCH, ACK, FINISH_EXP, TERMINATE }

public enum MonitorTime { 
    GenerateLocalBatch, ProcessRegionalBatch, EmitLocalBatch, NewLocalPACT, CommitLocalBatch, CommitHighBatch, CommitBatchLocally, SendBatchCommitToGrain, // local coordinator
    GenerateRegionalBatch, EmitRegionalBatch, NewRegionalPACT, CommitRegionalBatch, SendBatchCommitToLocal,          // regional coordinator
    RegisterBatchOnGrain, Execute, Prepare };                                                 // grain

public class Constants
{
    public const string localUser = "username";
    public const string remoteUser = "Administrator";

    // only for orleans transaction
    public const bool noDeadlock = false;

    // general config
    public const string SiloMembershipTable = "SnapperMembershipTable";
    public const string GrainDirectoryName = "SnapperGrainDirectory";
    public const string DefaultStreamProvider = "SnapperStreamProvider";
    public const string DefaultStreamStorage = "PubSubStore";
    public const int loggingBatchSize = 1;
    public const bool loggingBatching = false;
    public static TimeSpan deadlockTimeout = TimeSpan.FromMilliseconds(20);
    public const int numCachedOrleansClient = 4;
    // global silo config
    public const int numGlobalCoordPerCPU = 2;
    // regional silo config
    public const int numRegionalCoordPerCPU = 4;
    // local silo config
    public const int numCPUPerLocalSilo = 4;
    public const int numLocalCoordPerCPU = 4;
    public const int numPlacementManaerPerCPU = 2;
    public const int numReplicaPlacementManagerPerCPU = 4;
    public const int numMigrationWorkerPerCPU = 1;
    
    // benchmark config
    public const int numEpoch = 5;
    public const int numWarmupEpoch = 1;
    public const int epochDurationMSecs = 10000;
    public const int epochDurationForMigrationExp = 100000;

    // for grain migration experiment
    public const int beforeAndAfterDurationMSecs = 30000;
    public const long MeasurementDurationMSecs = 1000;     // calculate txn tp in every 1000 ms

    // other settings
    public const string workDir = @$"C:\Users\{remoteUser}\Desktop\SnapperX-Orleans";
    public const string dataDir = @$"C:\Users\{remoteUser}\Desktop\SnapperX-Orleans\data";
    public const string dataPath = workDir + @"\data\";
    public const string logPath = dataPath + @"log\";
    public const string resultPath = dataPath + "result.txt";
    public const string replicaResultPath = dataPath + "replica.txt";
    public const string credentialFile = dataPath + "AWS_credential.txt";
    
    // controller <==> worker / silo
    public const string controllerPublishPort = "5554";
    public const string controllerPullPort = "5555";

    // Redis
    public const int Redis_GrainDirectory = remoteUser == "Administrator" ? 0 : 0;
    public const int Redis_ClusterInfo = remoteUser == "Administrator" ? 0 : 1;
    public const int Redis_ReplicaInfo = remoteUser == "Administrator" ? 0 : 2;
    public const int Redis_EnvConfigureInfo = remoteUser == "Administrator" ? 0 : 3;
    public const int Redis_GrainPlacementChange = remoteUser == "Administrator" ? 0 : 4;    // keep record of the grain migration history

    // logging + replication config
    public const int numLogFilesPerPartition = 4;
    public const int numStreamsPerPartition = 10;
    public const int numEventChannelsPerPartition = 4;

    // port numbers
    public const string redis = "6379";      // cross region memory DB
    public const string kafka = "9092";      // cross region event channel
    public const string orleans = "8080";    // orleans dashboard

    // for workload generation
    public const int BASE_NUM_TRANSACTION = 50000;

    public const int maxNumReRun = 3;
    public const double sdSafeRange = 0.1;   // standard deviation should within the range of 10% * mean

    // for experiment automation
    public const string localWorkDir = @$"C:\Users\{localUser}\Desktop\SnapperX-Orleans\";
    public const string localDataPath = localWorkDir + @"data\";
    public const string localSnapperAmiInstanceID = "i-xxxxxxxxxxxxxxx";
    //public const string amiPrivateKeyFile = localDataPath + "snapper-ami.pem";
    //public const string privateKeyFile = localDataPath + "snapper.pem";
    public const string localInstanceInfo = localDataPath + "instance_info.txt";
    public const string localCredentialFile = localDataPath + "AWS_credential.txt";
    public const string localResultPath = localDataPath + "result.txt";
}
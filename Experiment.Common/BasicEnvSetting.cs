using MessagePack;
using Utilities;

namespace Experiment.Common;

[MessagePackObject]
public class BasicEnvSetting
{
    [Key(0)]
    public readonly bool isLocalTest;

    [Key(1)]
    public readonly bool isGrainMigrationExp;

    [Key(2)]
    public readonly int numRegion;

    [Key(3)]
    public readonly int numSiloPerRegion;

    [Key(4)]
    public readonly int numReplicaSiloPerRegion;

    [Key(5)]
    public readonly BenchmarkType benchmark;

    [Key(6)]
    public readonly ImplementationType implementationType;

    [Key(7)]
    public readonly bool doLogging;

    [Key(8)]
    public readonly bool inRegionReplication;

    [Key(9)]
    public readonly bool crossRegionReplication;

    /// <summary> if the read-only workload on replica is enabled </summary>
    [Key(10)]
    public readonly bool replicaWorkload;

    /// <summary> 
    /// determine how an ACT wait for batch of PACTs
    /// if true, an ACT can execute when previous batch completes, otherwise, wait for batch commit 
    /// </summary>
    [Key(11)]
    public readonly bool speculativeACT;

    /// <summary> 
    /// determine how a batch of PACT wait for previous batch
    /// if true, a batch can execute when previous batch completes, otherwise, wait for batch commit 
    /// </summary>
    [Key(12)]
    public readonly bool speculativeBatch;

    [Key(13)]
    public readonly double globalBatchSizeInMSecs;

    [Key(14)]
    public readonly double regionalBatchSizeInMSecs;

    [Key(15)]
    public readonly double localBatchSizeInMSecs;

    [Key(16)]
    public string redis_ConnectionString;

    [Key(17)]
    public string kafka_ConnectionString;

    [Key(18)]
    public string accessKey;

    [Key(19)]
    public string secretKey;

    /// <summary> the #CPUs for each type of role </summary>
    [Key(20)]
    public Dictionary<SnapperRoleType, int> roleSizes;

    /// <summary> public ip, regionIndex, siloIndex </summary>
    [Key(21)]
    public Dictionary<string, (int, int)> roleInfo;

    public BasicEnvSetting(
        bool isLocalTest,
        bool isGrainMigrationExp,
        int numRegion,
        int numSiloPerRegion,
        int numReplicaSiloPerRegion,
        BenchmarkType benchmark,
        ImplementationType implementationType,
        bool doLogging,
        bool inRegionReplication,
        bool crossRegionReplication,
        bool replicaWorkload,
        bool speculativeACT,
        bool speculativeBatch,
        double globalBatchSizeInMSecs,
        double regionalBatchSizeInMSecs,
        double localBatchSizeInMSecs) 
    { 
        this.isLocalTest = isLocalTest;
        this.isGrainMigrationExp = isGrainMigrationExp;

        this.numRegion = numRegion;
        this.numSiloPerRegion = numSiloPerRegion;
        this.numReplicaSiloPerRegion = numReplicaSiloPerRegion;
        this.benchmark = benchmark;
        this.implementationType = implementationType;

        this.doLogging = doLogging;
        this.inRegionReplication = inRegionReplication;
        this.crossRegionReplication = crossRegionReplication;
        this.replicaWorkload = replicaWorkload;

        this.speculativeACT = speculativeACT;
        this.speculativeBatch = speculativeBatch;

        this.globalBatchSizeInMSecs = globalBatchSizeInMSecs;
        this.regionalBatchSizeInMSecs = regionalBatchSizeInMSecs;
        this.localBatchSizeInMSecs = localBatchSizeInMSecs;
    }

    public void SetCredentialInfo(string redis_ConnectionString, string kafka_ConnectionString, string accessKey, string secretKey)
    {
        this.redis_ConnectionString = redis_ConnectionString;
        this.kafka_ConnectionString = kafka_ConnectionString;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public void SetClusterInfo(Dictionary<SnapperRoleType, int> roleSizes, Dictionary<string, (int, int)> roleInfo)
    {
        this.roleSizes = roleSizes;
        this.roleInfo = roleInfo;
    }

    /// <returns> numEpoch, numReRun </returns>
    public (int, int) GetNumEpochAndNumReRun()
    {
        if (isGrainMigrationExp) return (1, 1);
        else
        {
            if (isLocalTest) return (Constants.numEpoch, 1);
            else return (Constants.numEpoch, Constants.maxNumReRun);
        }
    }

    public void PrintContent()
    {
        Console.WriteLine($"BasicEnvSetting: isLocalTest             = {isLocalTest}");
        Console.WriteLine($"BasicEnvSetting: isGrainMigrationExp     = {isGrainMigrationExp}");
        Console.WriteLine($"BasicEnvSetting: numRegion               = {numRegion}");
        Console.WriteLine($"BasicEnvSetting: numSiloPerRegion        = {numSiloPerRegion}");
        Console.WriteLine($"BasicEnvSetting: numReplicaSiloPerRegion = {numReplicaSiloPerRegion}");
        Console.WriteLine($"BasicEnvSetting: benchmark               = {benchmark}");
        Console.WriteLine($"BasicEnvSetting: implementationType      = {implementationType}");
        Console.WriteLine($"BasicEnvSetting: doLogging               = {doLogging}");
        Console.WriteLine($"BasicEnvSetting: inRegionReplication     = {inRegionReplication}");
        Console.WriteLine($"BasicEnvSetting: crossRegionReplication  = {crossRegionReplication}");
        Console.WriteLine($"BasicEnvSetting: replicaWorkload         = {replicaWorkload}");
        Console.WriteLine($"BasicEnvSetting: speculativeACT          = {speculativeACT}");
        Console.WriteLine($"BasicEnvSetting: speculativeBatch        = {speculativeBatch}");
        Console.WriteLine($"BasicEnvSetting: globalBatchSize         = {globalBatchSizeInMSecs} ms");
        Console.WriteLine($"BasicEnvSetting: regionalBatchSize       = {regionalBatchSizeInMSecs} ms");
        Console.WriteLine($"BasicEnvSetting: localBatchSize          = {localBatchSizeInMSecs} ms");
    }

    public void PrintContent(StreamWriter file)
    {
        //file.WriteLine($"BasicEnvSetting: isLocalTest             = {isLocalTest}");
        //file.WriteLine($"BasicEnvSetting: isGrainMigrationExp     = {isGrainMigrationExp}");
        //file.WriteLine($"BasicEnvSetting: numRegion               = {numRegion}");
        file.WriteLine($"BasicEnvSetting: numSiloPerRegion        = {numSiloPerRegion}");
        file.WriteLine($"BasicEnvSetting: numReplicaSiloPerRegion = {numReplicaSiloPerRegion}");
        //file.WriteLine($"BasicEnvSetting: benchmark               = {benchmark}");
        //file.WriteLine($"BasicEnvSetting: implementationType      = {implementationType}");
        //file.WriteLine($"BasicEnvSetting: doLogging               = {doLogging}");
        file.WriteLine($"BasicEnvSetting: inRegionReplication     = {inRegionReplication}");
        //file.WriteLine($"BasicEnvSetting: crossRegionReplication  = {crossRegionReplication}");
        file.WriteLine($"BasicEnvSetting: replicaWorkload         = {replicaWorkload}");
        //file.WriteLine($"BasicEnvSetting: speculativeACT          = {speculativeACT}");
        //file.WriteLine($"BasicEnvSetting: speculativeBatch        = {speculativeBatch}");
        //file.WriteLine($"BasicEnvSetting: globalBatchSize         = {globalBatchSizeInMSecs} ms");
        //file.WriteLine($"BasicEnvSetting: regionalBatchSize       = {regionalBatchSizeInMSecs} ms");
        //file.WriteLine($"BasicEnvSetting: localBatchSize          = {localBatchSizeInMSecs} ms");
    }
}
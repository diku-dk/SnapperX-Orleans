using Experiment.Common;
using MessagePack;
using Utilities;

namespace TestApp.Workload;

[MessagePackObject]
public class EnvSetting : IEnvSetting
{
    [Key(0)]
    public readonly BasicEnvSetting basic;

    [Key(1)]
    public readonly int numMasterPartitionPerLocalSilo;

    [Key(2)]
    public readonly int numGrainPerPartition;

    public EnvSetting(BasicEnvSetting basic, int numMasterPartitionPerLocalSilo, int numGrainPerPartition)
    {
        this.basic = basic;
        this.numMasterPartitionPerLocalSilo = numMasterPartitionPerLocalSilo;
        this.numGrainPerPartition = numGrainPerPartition;
    }

    public EnvSetting(
        bool isLocalTest, bool isGrainMigrationExp, int numRegion, int numSiloPerRegion, int numReplicaSiloPerRegion,
        BenchmarkType benchmark, ImplementationType implementationType, bool doLogging,
        bool inRegionReplication, bool crossRegionReplication, bool replicaWorkload,
        bool speculativeACT, bool speculativeBatch,
        double globalBatchSizeInMSecs, double regionalBatchSizeInMSecs, double localBatchSizeInMSecs,
        int numMasterPartitionPerLocalSilo, int numGrainPerPartition)
    {
        basic = new BasicEnvSetting(isLocalTest, isGrainMigrationExp, numRegion, numSiloPerRegion, numReplicaSiloPerRegion,
            benchmark, implementationType, doLogging,
            inRegionReplication, crossRegionReplication, replicaWorkload,
            speculativeACT, speculativeBatch,
            globalBatchSizeInMSecs, regionalBatchSizeInMSecs, localBatchSizeInMSecs);
        this.numMasterPartitionPerLocalSilo = numMasterPartitionPerLocalSilo;
        this.numGrainPerPartition = numGrainPerPartition;
    }

    public int GetNumPartitionPerSilo() => numMasterPartitionPerLocalSilo;

    public BasicEnvSetting GetBasic() => basic;

    public void SetCredentialInfo(string redis_ConnectionString, string kafka_ConnectionString, string accessKey, string secretKey)
        => basic.SetCredentialInfo(redis_ConnectionString, kafka_ConnectionString, accessKey, secretKey);

    public void SetClusterInfo(Dictionary<SnapperRoleType, int> roleSizes, Dictionary<string, (int, int)> roleInfo)
        => basic.SetClusterInfo(roleSizes, roleInfo);

    public int GetTotalNumMasterPartitions() => numMasterPartitionPerLocalSilo * basic.numSiloPerRegion * basic.numRegion;

    public void PrintContent()
    {
        Console.WriteLine();
        Console.WriteLine($"=========================================================================================");
        basic.PrintContent();
        Console.WriteLine($"     EnvSetting: numMasterPartitionPerLocalSilo = {numMasterPartitionPerLocalSilo}");
        Console.WriteLine($"     EnvSetting: numGrainPerPartition           = {numGrainPerPartition}");
        Console.WriteLine($"=========================================================================================");
        Console.WriteLine();
    }

    public void PrintContent(StreamWriter file)
    {
        basic.PrintContent(file);
        file.WriteLine($"     EnvSetting: numMasterPartitionPerLocalSilo = {numMasterPartitionPerLocalSilo}");
        file.WriteLine($"     EnvSetting: numGrainPerPartition           = {numGrainPerPartition}");
    }
}
using Experiment.Common;
using MessagePack;
using Utilities;

namespace TestApp.Workload;

[MessagePackObject(keyAsPropertyName: true)]
public class WorkloadConfigure : IWorkloadConfigure
{
    public readonly int numGrainPerTxn;

    public readonly int txnDepth;

    /// <summary> the possibility that the access mode on a grain is no-op </summary>
    public readonly int noOpPercent;

    /// <summary> the possibility that the access mode on a grain is read-only </summary>
    public readonly int readOnlyPercent;

    /// <summary> the possibility of a write to be a key update operation </summary>
    public readonly int updatePercent;

    /// <summary> the possibility of a write to be a key delete operation </summary>
    public readonly int deletePercent;

    public readonly Dictionary<TestAppTxnType, int> txnTypes;

    public readonly int numPartitionPerSiloPerTxn;

    public readonly int numSiloPerTxn;

    public readonly int numRegionPerTxn;

    /// <summary> the possibility of a transaction to be a PACT, [0, 100] </summary>
    public readonly int pactPercent;

    /// <summary> the possibility of a transaction to be a multi-silo transaction, [0, 100] </summary>
    public readonly int multiSiloPercent;

    /// <summary> the possibility of a transaction to be a multi-region transaction, [0, 100] </summary>
    public readonly int multiRegionPercent;

    /// <summary> the probability that a grain is selected from the hot set, [0, 100] </summary>
    public readonly int hotGrainPercent;

    /// <summary> the percentage of hot grains in each partition of grains, (0, 100] </summary>
    public readonly double hotPercentPerPartition;

    /// <summary> the pipe line size of each ACT thread on each worker </summary>
    public readonly int actPipeSize;

    /// <summary> the pipe line size of each PACT thread on each worker </summary>
    public readonly int pactPipeSize;

    /// <summary> the number of ACT consumer threads on each worker </summary>
    public int numActConsumer;

    /// <summary> the number of PACT consumer threads on each worker </summary>
    public int numPactConsumer;

    /// <summary> the pipe line size of each grain migration thread on each worker </summary>
    public int migrationPipeSize;

    /// <summary> the number of grain migration threads on each worker </summary>
    public int numMigrationConsumer;

    public WorkloadConfigure(
        int numGrainPerTxn,

        int txnDepth,
        int noOpPercent,
        int readOnlyPercent,
        int updatePercent,
        int deletePercent,

        Dictionary<TestAppTxnType, int> txnTypes,

        int numPartitionPerSiloPerTxn,
        int numSiloPerTxn,
        int numRegionPerTxn,

        int pactPercent,
        int multiSiloPercent,
        int multiRegionPercent,

        int hotGrainPercent,
        double hotPercentPerPartition,

        int actPipeSize,
        int pactPipeSize,
        int numActConsumer,
        int numPactConsumer,

        int migrationPipeSize,
        int numMigrationConsumer)
    {
        this.numGrainPerTxn = numGrainPerTxn;

        this.txnDepth = txnDepth;
        this.noOpPercent = noOpPercent;
        this.readOnlyPercent = readOnlyPercent;
        this.updatePercent = updatePercent;
        this.deletePercent = deletePercent;

        this.txnTypes = txnTypes;

        this.numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxn;
        this.numSiloPerTxn = numSiloPerTxn;
        this.numRegionPerTxn = numRegionPerTxn;

        this.pactPercent = pactPercent;
        this.multiSiloPercent = multiSiloPercent;
        this.multiRegionPercent = multiRegionPercent;

        this.hotGrainPercent = hotGrainPercent;
        this.hotPercentPerPartition = hotPercentPerPartition;

        this.actPipeSize = actPipeSize;
        this.pactPipeSize = pactPipeSize;
        this.numActConsumer = numActConsumer;
        this.numPactConsumer = numPactConsumer;

        this.migrationPipeSize = migrationPipeSize;
        this.numMigrationConsumer = numMigrationConsumer;
    }

    /// <summary> this is for the read-only workload on replicas </summary>
    public WorkloadConfigure(
        int numGrainPerTxn,

        int txnDepth,

        Dictionary<TestAppTxnType, int> txnTypes,

        int numPartitionPerSiloPerTxn,
        int numSiloPerTxn,

        int pactPercent,
        int multiSiloPercent,

        int hotGrainPercent,
        double hotPercentPerPartition,

        int actPipeSize,
        int pactPipeSize,
        int numActConsumer,
        int numPactConsumer,

        int migrationPipeSize,
        int numMigrationConsumer)
    {
        this.numGrainPerTxn = numGrainPerTxn;

        this.txnDepth = txnDepth;

        this.txnTypes = txnTypes;

        this.numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxn;
        this.numSiloPerTxn = numSiloPerTxn;
        this.numRegionPerTxn = 1;

        this.pactPercent = pactPercent;
        this.multiSiloPercent = multiSiloPercent;

        this.hotGrainPercent = hotGrainPercent;
        this.hotPercentPerPartition = hotPercentPerPartition;

        this.actPipeSize = actPipeSize;
        this.pactPipeSize = pactPipeSize;
        this.numActConsumer = numActConsumer;
        this.numPactConsumer = numPactConsumer;

        this.migrationPipeSize = migrationPipeSize;
        this.numMigrationConsumer = numMigrationConsumer;
    }

    public int GetActPipeSize() => pactPercent == 100 ? 0 : actPipeSize;

    public int GetPactPipeSize() => pactPercent == 0 ? 0 : pactPipeSize;

    public int GetNumActConsumer() => numActConsumer;

    public int GetNumPactConsumer() => numPactConsumer;

    public int GetMigrationPipeSize() => migrationPipeSize;

    public int GetNumMigrationConsumer() => numMigrationConsumer;
}
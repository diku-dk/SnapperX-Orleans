using Experiment.Common;
using MessagePack;
using Utilities;

namespace SmallBank.Workload;

[MessagePackObject(keyAsPropertyName: true)]
public class WorkloadConfigure : IWorkloadConfigure
{
    /// <summary> the number of grains to access for each transaction </summary>
    public readonly int numGrainPerTxn;

    /// <summary> the number of keys to access on each selected grain </summary>
    public readonly int numKeyToAccessPerGrain;

    public readonly Dictionary<SmallBankTxnType, int> txnTypes;

    public readonly int numPartitionPerSiloPerTxn;

    public readonly int numSiloPerTxn;

    public readonly int numRegionPerTxn;

    /// <summary> the possibility of a transaction to be a PACT, [0, 100] </summary>
    public readonly int pactPercent;

    /// <summary> the possibility of a transaction to be a multi-silo transaction, [0, 100] </summary>
    public readonly int multiSiloPercent;

    /// <summary> the possibility of a transaction to be a multi-region transaction, [0, 100] </summary>
    public readonly int multiRegionPercent;

    /// <summary> the probability that an access to a key is a read operation, instead of a write </summary>
    public readonly int readKeyPercent;

    /// <summary> the probability that an accessed key is selected from the hot set, [0, 100] </summary>
    public readonly int hotKeyPercent;

    /// <summary> the percentage of hot keys among all keys on a grain, (0, 100] </summary>
    public readonly double hotPercentPerGrain;

    /// <summary> the probability that a grain is selected from the hot set, [0, 100] </summary>
    public readonly int hotGrainPercent;

    /// <summary> the percentage of hot grains among all grains in each partition, (0, 100] </summary>
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
        int numKeyToAccessPerGrain,

        Dictionary<SmallBankTxnType, int> txnTypes,

        int numPartitionPerSiloPerTxn,
        int numSiloPerTxn,
        int numRegionPerTxn,

        int pactPercent,
        int multiSiloPercent,
        int multiRegionPercent,

        int readKeyPercent,
        int hotKeyPercent,
        double hotPercentPerGrain,
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
        this.numKeyToAccessPerGrain = numKeyToAccessPerGrain;

        this.txnTypes = txnTypes;

        this.numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxn;
        this.numSiloPerTxn = numSiloPerTxn;
        this.numRegionPerTxn = numRegionPerTxn;

        this.pactPercent = pactPercent;
        this.multiSiloPercent = multiSiloPercent;
        this.multiRegionPercent = multiRegionPercent;

        this.readKeyPercent = readKeyPercent;
        this.hotKeyPercent = hotKeyPercent;
        this.hotPercentPerGrain = hotPercentPerGrain;
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
        int numKeyToAccessPerGrain,

        Dictionary<SmallBankTxnType, int> txnTypes,

        int numPartitionPerSiloPerTxn,
        int numSiloPerTxn,

        int pactPercent,
        int multiSiloPercent,

        int hotKeyPercent,
        double hotPercentPerGrain,
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
        this.numKeyToAccessPerGrain = numKeyToAccessPerGrain;

        this.txnTypes = txnTypes;

        this.numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxn;
        this.numSiloPerTxn = numSiloPerTxn;
        this.numRegionPerTxn = 1;

        this.pactPercent = pactPercent;
        this.multiSiloPercent = multiSiloPercent;
        this.multiRegionPercent = 0;

        this.readKeyPercent = 100;
        this.hotKeyPercent = hotKeyPercent;
        this.hotPercentPerGrain = hotPercentPerGrain;
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
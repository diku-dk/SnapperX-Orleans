using Concurrency.Common;

namespace Concurrency.Interface.GrainPlacement;

public interface IGrainMigrationWorker : IGrainWithGuidCompoundKey
{
    Task<GrainMigrationResult> MigrateGrain(GrainID grainID, int targetPartitionID);

    Task<GrainMigrationResult> DoMigration(GrainID grainID, int targetPartitionID);

    Task UpdateUserGrainInfoInCache(GrainID grainID, int targetPartitionID);

    Task<AggregatedExperimentData> CheckGC();
}
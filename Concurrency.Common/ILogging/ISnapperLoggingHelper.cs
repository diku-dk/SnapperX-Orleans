using Confluent.Kafka;
using Orleans.Streams;

namespace Concurrency.Common.ILogging;

public interface ISnapperLoggingHelper
{
    Task Init(string siloID, bool doLogging, bool inRegionReplication, bool crossRegionReplication, int numMasterPartitionPerLocalSilo, IStreamProvider? streamProvider, ProducerConfig? kafkaConfig);

    bool DoLogging();

    /// <summary> 
    /// for PACT, the info is persisted when a new batch is generated, it includes all accessed grains 
    /// for ACT, the info ia persisted before an ACT doing 2PC, it includes all writer grains
    /// </summary>
    Task InfoLog(SnapperID id, HashSet<GrainID> grains);

    /// <summary>
    /// for PACT, the prepare log is written after completing a whole batch
    /// for ACT, the prepare log is written when receiving prepare 2PC message
    /// </summary>
    Task PrepareLog(GrainID grainID, SnapperID id, DateTime timestamp, SnapperID prevId, DateTime prevTimestamp, byte[] updatesOnDictionary, byte[] updateOnReferences, byte[] updatesOnList);

    /// <summary>
    /// for PACT, one commit log is persisited for one batch
    /// for ACT, one commit log is persisted for one ACT
    /// </summary>
    Task CommitLog(SnapperID id);

    bool DoReplication();

    /// <summary> the info event is sent after committing a batch / an ACT, only includes writer grains </summary>
    Task InfoEvent(SnapperID id, HashSet<GrainID> grains);

    /// <summary> the prepare event is sent when the prepare log is persisted </summary>
    Task PrepareEvent(GrainID grainID, SnapperID id, DateTime timestamp, SnapperID prevId, DateTime prevTimestamp, byte[] updatesOnDictionary, byte[] updateOnReferences, byte[] updatesOnList);
}
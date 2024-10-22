using Concurrency.Common.Logging;
using Confluent.Kafka;
using Orleans.Streams;

namespace Concurrency.Common.ILogging;

public interface ISnapperSubscriber
{
    Task Init(
        bool inRegionReplication, 
        bool crossRegionReplication,
        int totalNumMasterPartitions,
        Dictionary<string, List<Guid>> pmList, 
        Dictionary<int, string> partitionIDToSiloID, 
        IStreamProvider? streamProvider, 
        ConsumerConfig? kafkaConfig);

    (List<IAsyncStream<SnapperLog>>, List<IConsumer<GrainID, SnapperLog>>) GetSubscriptions(string siloID, Guid pmID);
}
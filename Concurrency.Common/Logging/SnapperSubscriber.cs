using Concurrency.Common.ILogging;
using Confluent.Kafka;
using Orleans.Runtime;
using Orleans.Streams;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.Logging;

public class SnapperSubscriber : ISnapperSubscriber
{
    /// <summary> silo ID, PM ID, the list of streams this PM should subscribe </summary>
    Dictionary<string, Dictionary<Guid, List<IAsyncStream<SnapperLog>>>> streamsPerPM;

    /// <summary> silo ID, PM ID, the list of kafka channels this PM should subscribe </summary>
    Dictionary<string, Dictionary<Guid, List<IConsumer<GrainID, SnapperLog>>>> channelsPerPM;

    public SnapperSubscriber()
    {
        streamsPerPM = new Dictionary<string, Dictionary<Guid, List<IAsyncStream<SnapperLog>>>>();
        channelsPerPM = new Dictionary<string, Dictionary<Guid, List<IConsumer<GrainID, SnapperLog>>>>();
    }

    async Task ISnapperSubscriber.Init(bool inRegionReplication, bool crossRegionReplication,
        int totalNumMasterPartitions,
        Dictionary<string, List<Guid>> pmList,          // replica silo ID, list of PMs in the replica silo
        Dictionary<int, string> partitionIDToSiloID,    // replica partition ID, replica silo ID
        IStreamProvider? streamProvider, ConsumerConfig? kafkaConfig)
    {
        // add entry for each PM
        foreach (var item in pmList)
        {
            streamsPerPM.Add(item.Key, new Dictionary<Guid, List<IAsyncStream<SnapperLog>>>());
            channelsPerPM.Add(item.Key, new Dictionary<Guid, List<IConsumer<GrainID, SnapperLog>>>());
            foreach (var pm in item.Value)
            {
                streamsPerPM[item.Key].Add(pm, new List<IAsyncStream<SnapperLog>>());
                channelsPerPM[item.Key].Add(pm, new List<IConsumer<GrainID, SnapperLog>>());
            }
        }

        if (inRegionReplication)
        {
            var rnd = new Random();
            Debug.Assert(streamProvider != null);

            // for info logs, all replica silos in the whole region should make sure to subscribe to all info log streams
            var siloList = pmList.Keys.ToList();
            for (var partitionID = 0; partitionID < totalNumMasterPartitions; partitionID++)
            {
                for (var index = 0; index < Constants.numStreamsPerPartition; index++)
                {
                    // select a replica silo
                    string siloID;
                    if (partitionIDToSiloID.ContainsKey(partitionID)) siloID = partitionIDToSiloID[partitionID];
                    else siloID = siloList[rnd.Next(0, siloList.Count)];

                    // select a replica pm
                    var selectedIndex = (partitionID * Constants.numStreamsPerPartition + index) % pmList[siloID].Count;
                    var pm = pmList[siloID][selectedIndex];

                    var stream = streamProvider.GetStream<SnapperLog>(StreamId.Create(SnapperLogType.Info.ToString() + "-" + partitionID.ToString(), Helper.ConvertIntToGuid(index)));
                    streamsPerPM[siloID][pm].Add(stream);
                }
            }
        }

        if (crossRegionReplication)
        {
            var rnd = new Random();
            Debug.Assert(kafkaConfig != null);

            // for prepare logs, each silo only subscribes to related partitions
            foreach (var item in partitionIDToSiloID)
            {
                var partitionID = item.Key;
                var siloID = item.Value;

                Debug.Assert(pmList.ContainsKey(siloID));
                for (var index = 0; index < Constants.numEventChannelsPerPartition; index++)
                {
                    // for each index of a partition, randomly select a PM
                    var selectedIndex = rnd.Next(0, pmList[siloID].Count);
                    var pm = pmList[siloID][selectedIndex];

                    var consumerBuilder = new ConsumerBuilder<GrainID, SnapperLog>(kafkaConfig).SetValueDeserializer(new SnapperLogSerializer());
                    var consumer = consumerBuilder.Build();
                    consumer.Subscribe(SnapperLogType.Prepare.ToString() + "-" + partitionID.ToString() + "-" + index.ToString());
                    channelsPerPM[siloID][pm].Add(consumer);
                }
            }

            // for info logs, all replica silos in the whole region should make sure to subscribe to all info log channels
            var siloList = pmList.Keys.ToList();
            for (var partitionID = 0; partitionID < totalNumMasterPartitions; partitionID++)
            {
                for (var index = 0; index < Constants.numEventChannelsPerPartition; index++)
                {
                    // select a replica silo
                    string siloID;
                    if (partitionIDToSiloID.ContainsKey(partitionID)) siloID = partitionIDToSiloID[partitionID];
                    else siloID = siloList[rnd.Next(0, siloList.Count)];

                    // randomly select a replica pm
                    var selectedIndex = (partitionID * Constants.numEventChannelsPerPartition + index) % pmList[siloID].Count;
                    var pm = pmList[siloID][selectedIndex];

                    var consumerBuilder = new ConsumerBuilder<GrainID, SnapperLog>(kafkaConfig).SetValueDeserializer(new SnapperLogSerializer());
                    var consumer = consumerBuilder.Build();
                    consumer.Subscribe(SnapperLogType.Info.ToString() + "-" + partitionID.ToString() + "-" + index.ToString());
                    channelsPerPM[siloID][pm].Add(consumer);
                }
            }
        }
    }

    /// <returns> list of streams the PM should subscribe, list of channels the PM should subscribe </returns>
    (List<IAsyncStream<SnapperLog>>, List<IConsumer<GrainID, SnapperLog>>) ISnapperSubscriber.GetSubscriptions(string siloID, Guid pmID) 
        => (streamsPerPM[siloID][pmID], channelsPerPM[siloID][pmID]);
}
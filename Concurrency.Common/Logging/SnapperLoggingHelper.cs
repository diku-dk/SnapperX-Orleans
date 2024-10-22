using Concurrency.Common.ILogging;
using MessagePack;
using Orleans.Streams;
using System.Diagnostics;
using Utilities;
using Confluent.Kafka;
using Orleans.Runtime;
using Concurrency.Common.ICache;

namespace Concurrency.Common.Logging;

public class SnapperLoggingHelper : ISnapperLoggingHelper
{
    bool doLogging;

    bool inRegionReplication;

    bool crossRegionReplication;

    readonly ISnapperClusterCache snapperClusterCache;

    IStreamProvider? streamProvider;

    /// <summary> for log file writing </summary>
    Dictionary<SnapperLogType, List<ISnapperLogger>> loggers;

    /// <summary> 
    /// for in-region replication of info logs, (log type, partition ID, streams) 
    /// stream ID: log type "-" partition ID, use index as guid
    /// </summary>
    Dictionary<int, List<IAsyncStream<SnapperLog>>> streams;

    List<int> partitionIDs; 

    /// <summary> 
    /// for cross-region replication, (log type, partition ID, event producers)
    /// topic: log type "-" partition ID "-" index
    /// key: GrainID, in kafka, messages are guranteed to be processed in order if they share the same key
    /// </summary>
    Dictionary<SnapperLogType, Dictionary<int, List<IProducer<GrainID, SnapperLog>>>> channels;

    public SnapperLoggingHelper(ISnapperClusterCache snapperClusterCache)
    {
        this.snapperClusterCache = snapperClusterCache;

        loggers = new Dictionary<SnapperLogType, List<ISnapperLogger>>
        {
            { SnapperLogType.Info, new List<ISnapperLogger>() },
            { SnapperLogType.Prepare, new List<ISnapperLogger>() },
            { SnapperLogType.Commit, new List<ISnapperLogger>() }
        };

        partitionIDs = new List<int>();
        streams = new Dictionary<int, List<IAsyncStream<SnapperLog>>>();

        channels = new Dictionary<SnapperLogType, Dictionary<int, List<IProducer<GrainID, SnapperLog>>>> 
        {
            { SnapperLogType.Info, new Dictionary<int, List<IProducer<GrainID, SnapperLog>>>() },
            { SnapperLogType.Prepare, new Dictionary<int, List<IProducer<GrainID, SnapperLog>>>() }
        };
    }

    async Task ISnapperLoggingHelper.Init(string siloID, bool doLogging, bool inRegionReplication, bool crossRegionReplication, int numMasterPartitionPerLocalSilo, IStreamProvider? streamProvider, ProducerConfig? kafkaConfig)
    {
        this.doLogging = doLogging;
        this.inRegionReplication = inRegionReplication;
        this.crossRegionReplication = crossRegionReplication;
        
        if (doLogging)
        {
            // create the log folder if does not exist
            if (!Directory.Exists(Constants.logPath)) Directory.CreateDirectory(Constants.logPath);

            // init log files
            var numLogFilePerType = Constants.numLogFilesPerPartition * numMasterPartitionPerLocalSilo;
            foreach (var item in loggers)
            {
                var type = item.Key;
                var loggerGroup = item.Value;

                loggerGroup.ForEach(logger => logger.CleanUp());
                loggerGroup.Clear();

                for (var i = 0; i < numLogFilePerType; i++)
                {
                    var logger = new SnapperLogger();
                    await logger.Init($"{Helper.ReFormSiloID(siloID)}-{type}-{i}");
                    loggerGroup.Add(logger);
                }
            }
        }

        var allMasterPartitionIDs = snapperClusterCache.getAllMasterPartitionIDs();
        if (inRegionReplication)
        {
            Debug.Assert(streamProvider != null);
            this.streamProvider = streamProvider;

            // init streams
            allMasterPartitionIDs.ForEach(partitionID =>
            {
                partitionIDs.Add(partitionID);
                streams.Add(partitionID, new List<IAsyncStream<SnapperLog>>());
                for (var i = 0; i < Constants.numStreamsPerPartition; i++)
                {
                    var stream = streamProvider.GetStream<SnapperLog>(StreamId.Create(SnapperLogType.Info.ToString() + "-" + partitionID.ToString(), Helper.ConvertIntToGuid(i)));
                    streams[partitionID].Add(stream);
                }
            });
        }

        if (crossRegionReplication)
        {
            Debug.Assert(kafkaConfig != null);

            // init channels
            foreach (var item in channels)
            {
                var logType = item.Key;
                var channelGroup = item.Value;

                allMasterPartitionIDs.ForEach(partitionID =>
                {
                    channelGroup.Add(partitionID, new List<IProducer<GrainID, SnapperLog>>());
                    for (var i = 0; i < Constants.numEventChannelsPerPartition; i++)
                    {
                        var kafkaBuilder = new ProducerBuilder<GrainID, SnapperLog>(kafkaConfig).SetValueSerializer(new SnapperLogSerializer());
                        var kafkaProducer = kafkaBuilder.Build();
                        channelGroup[partitionID].Add(kafkaProducer);
                    }
                });
            }
        }
    }

    bool ISnapperLoggingHelper.DoLogging() => doLogging;

    bool ISnapperLoggingHelper.DoReplication() => inRegionReplication || crossRegionReplication;

    // for logging ===========================================================================================================================
    ISnapperLogger SelectLogger(SnapperLogType type, GrainID? grainID = null)
    {
        var loggerGroup = loggers[type];
        int index;
        if (grainID != null) index = Helper.MapGuidToServiceID(grainID.id, loggerGroup.Count);
        else index = new Random().Next(0, loggerGroup.Count);
        return loggerGroup[index];
    }

    async Task ISnapperLoggingHelper.InfoLog(SnapperID id, HashSet<GrainID> grains)
    {
        var content = MessagePackSerializer.Serialize(new InfoLog(id, grains));
        var logger = SelectLogger(SnapperLogType.Info, grains.First());
        await logger.Write(MessagePackSerializer.Serialize(new SnapperLog(SnapperLogType.Info, content)));
    }

    async Task ISnapperLoggingHelper.PrepareLog(GrainID grainID, SnapperID id, DateTime timestamp, SnapperID prevId, DateTime prevTimestamp, byte[] updatesOnDictionary, byte[] updateOnReferences, byte[] updatesOnList)
    {
        var content = MessagePackSerializer.Serialize(new PrepareLog(grainID, id, timestamp, prevId, prevTimestamp, updatesOnDictionary, updateOnReferences, updatesOnList));
        var logger = SelectLogger(SnapperLogType.Prepare, grainID);
        await logger.Write(MessagePackSerializer.Serialize(new SnapperLog(SnapperLogType.Prepare, content)));
    }

    async Task ISnapperLoggingHelper.CommitLog(SnapperID id)
    {
        var content = MessagePackSerializer.Serialize(new CommitLog(id));
        var logger = SelectLogger(SnapperLogType.Commit);
        await logger.Write(MessagePackSerializer.Serialize(new SnapperLog(SnapperLogType.Commit, content)));
    }

    // for replication ===========================================================================================================================
    async Task ISnapperLoggingHelper.InfoEvent(SnapperID id, HashSet<GrainID> grains)
    {
        var replicaGrains = grains.Select(x =>
        {
            Debug.Assert(!string.IsNullOrEmpty(x.className));
            return new GrainID(x.id, Helper.GetReplicaClassName(x.className));
        }).ToHashSet();
        var log = new SnapperLog(SnapperLogType.Info, MessagePackSerializer.Serialize(new InfoLog(id, replicaGrains)));
        await EmitLog(log, grains.First());
    }

    async Task ISnapperLoggingHelper.PrepareEvent(GrainID grainID, SnapperID id, DateTime timestamp, SnapperID prevId, DateTime prevTimestamp, byte[] updatesOnDictionary, byte[] updateOnReferences, byte[] updatesOnList)
    {
        var log = new SnapperLog(SnapperLogType.Prepare, MessagePackSerializer.Serialize(new PrepareLog(grainID, id, timestamp, prevId, prevTimestamp, updatesOnDictionary, updateOnReferences, updatesOnList)));
        await EmitLog(log, grainID);
    }

    async Task EmitLog(SnapperLog log, GrainID grainID)
    {
        try
        {
            var tasks = new List<Task>();
            var partitionID = snapperClusterCache.GetPartitionID(grainID);
            if (inRegionReplication)
            {
                if (log.type == SnapperLogType.Info)
                {
                    var selectedPartitionID = partitionID;
                    if (!streams.ContainsKey(partitionID)) selectedPartitionID = partitionIDs[new Random().Next(0, partitionIDs.Count)];

                    var index = new Random().Next(0, Constants.numStreamsPerPartition);
                    var stream = streams[selectedPartitionID][index];
                    tasks.Add(stream.OnNextAsync(log));
                }
                else if (log.type == SnapperLogType.Prepare)
                {
                    Debug.Assert(!string.IsNullOrEmpty(grainID.className));
                    Debug.Assert(streamProvider != null);
                    var className = Helper.GetReplicaClassName(grainID.className);
                    var stream = streamProvider.GetStream<SnapperLog>(StreamId.Create(SnapperLogType.Prepare.ToString() + "-" + className, grainID.id));
                    tasks.Add(stream.OnNextAsync(log));

                    //var preparelog = MessagePackSerializer.Deserialize<PrepareLog>(log.content);
                    //Console.WriteLine($"Prepare: id = {preparelog.id.id}, grainID = {Helper.ConvertGuidToInt(preparelog.grainID.id)}, emit to {stream.StreamId}");
                }
            }

            if (crossRegionReplication)
            {
                var index = new Random().Next(0, Constants.numEventChannelsPerPartition);
                var channel = channels[log.type][partitionID][index];
                tasks.Add(channel.ProduceAsync(log.type.ToString() + "-" + partitionID.ToString() + "-" + index.ToString(), new Message<GrainID, SnapperLog>
                {
                    Timestamp = new Timestamp(DateTime.UtcNow),
                    Key = grainID,
                    Value = log
                }));
            }

            await Task.WhenAll(tasks);
        }
        catch (Exception e) 
        {
            Console.WriteLine($"EmitLog: {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }
}
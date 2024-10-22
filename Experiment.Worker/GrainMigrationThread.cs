using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.GrainPlacement;
using Experiment.Common;
using MathNet.Numerics.Statistics;
using System.Diagnostics;
using Utilities;

namespace Experiment.Worker;

internal class GrainMigrationThread
{
    readonly bool isReplica;
    readonly int pipeSize;
    readonly Stopwatch globalWatch;
    readonly Barrier[] barriers;

    int numEmit;
    int numCommit;
    List<Task<GrainMigrationResult>> tasks;
    Dictionary<Task, DateTime> taskStartTime;
    Queue<(IGrainMigrationWorker, GrainID, int)> queue;                            // selected grain migration worker, grain ID to migrate, the target partition ID
    
    List<double> latencies = new List<double>();                                   // the end-to-end latency of each grain migration request
    SortedDictionary<GrainMigrationTimeInterval, List<double>> intervalLatencies;  // the latency of each interval

    public GrainMigrationThread(
        bool isReplica,
        int migrationPipeSize,
        IClusterClient client,
        IEnvConfigure envConfigureHelper,
        Barrier[] barriers)
    {
        this.isReplica = isReplica;
        this.pipeSize = migrationPipeSize;
        globalWatch = new Stopwatch();
        this.barriers = barriers;

        numEmit = 0;
        numCommit = 0;
        tasks = new List<Task<GrainMigrationResult>>();
        taskStartTime = new Dictionary<Task, DateTime>();
        latencies = new List<double>();
        intervalLatencies = new SortedDictionary<GrainMigrationTimeInterval, List<double>>();
        foreach (GrainMigrationTimeInterval i in Enum.GetValues(typeof(GrainMigrationTimeInterval))) intervalLatencies.Add(i, new List<double>());
        
        // pre-generate all grain migration requests
        if (!isReplica)
        {
            (var grainIDToPartitionID, var partitionIDToMasterInfo, var grainIDToMigrationWorker) = envConfigureHelper.GetGrainPlacementInfo();

            queue = new Queue<(IGrainMigrationWorker, GrainID, int)>();

            for (var i = 0; i < grainIDToPartitionID.Count; i++)   // TODO
            {
                (var grainID, var newPartitionID, var mwGuid, var mwLocation) = GetOneRandomGrain(grainIDToPartitionID, partitionIDToMasterInfo, grainIDToMigrationWorker);
                var mw = client.GetGrain<IGrainMigrationWorker>(mwGuid, mwLocation, typeof(GrainMigrationWorker).FullName);
                queue.Enqueue((mw, grainID, newPartitionID));
            }

            Console.WriteLine($"GrainMigrationThread: pipe size = {pipeSize}, {queue.Count} grain migration requests are generate. ");
        }
        else
        {
            (var replicaGrainIDToPartitionID, var partitionIDToReplicaInfo) = envConfigureHelper.GetReplicaGrainPlacementInfo();


        }
    }

    public async Task RunEpoch(int eIndex, int threadIndex)
    {
        //Console.WriteLine($"GrainMigrationThread {threadIndex}: Wait for {Constants.beforeAndAfterDurationMSecs / 1000}s ...");
        //await Task.Delay(Constants.beforeAndAfterDurationMSecs);
        barriers[eIndex].SignalAndWait();
        globalWatch.Restart();
        var start = DateTime.UtcNow;
        (IGrainMigrationWorker, GrainID, int) task;
        do
        {
            while (tasks.Count < pipeSize && queue.TryDequeue(out task))
            {
                var startTxnTime = DateTime.UtcNow;
                var t = task.Item1.MigrateGrain(task.Item2, task.Item3);
                tasks.Add(t);
                taskStartTime.Add(t, startTxnTime);
                numEmit++;
            }
            if (tasks.Count != 0) await WaitForTaskAsync();
        }
        while (globalWatch.ElapsedMilliseconds < Constants.epochDurationForMigrationExp && queue.Count != 0);

        while (tasks.Count != 0) await WaitForTaskAsync();
        var time = (DateTime.UtcNow - start).TotalMilliseconds;
        globalWatch.Stop();

        var average = latencies.Count != 0 ? latencies.Mean() : 0;

        Console.WriteLine($"GrainMigrationThread: numEmit = {numEmit}, total tp = {(int)(numEmit * 1000 / time)}");
        Console.WriteLine($"GrainMigrationThread: numCommit = {numCommit}, commit tp = {(int)(numCommit * 1000 / time)}, average latency = {Helper.ChangeFormat(average, 0)}ms");
        
        foreach (var item in intervalLatencies)
            Console.WriteLine($"GrainMigrationThread: {item.Key} {Helper.ChangeFormat(item.Value.Mean(), 2)}ms");
    }

    async Task WaitForTaskAsync()
    {
        var task = await Task.WhenAny(tasks);
        var endTxnTime = DateTime.UtcNow;
        var succeed = true;
        try
        {
            await task;
        }
        catch (SnapperException e)
        {
            succeed = false;
            Debug.Assert(e.exceptionType == ExceptionType.GrainMigration);
        }
        catch (Exception e)
        {
            Console.WriteLine($"{e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }

        if (succeed)
        {
            latencies.Add((endTxnTime - taskStartTime[task]).TotalMilliseconds);

            Debug.Assert(task.Result.intervalTime.Count != 0);
            foreach (var item in task.Result.intervalTime) intervalLatencies[item.Key].Add(item.Value);
            
            numCommit++;
        }
        tasks.Remove(task);
        taskStartTime.Remove(task);
    }

    /// <summary> 
    /// a randomly selected grain ID
    /// the target new partition ID
    /// the corresponding migration worker's guid and location 
    /// </summary>
    (GrainID, int, Guid, string) GetOneRandomGrain(
        Dictionary<GrainID, int> grainIDToPartitionID, 
        Dictionary<int, (string, string)> partitionIDToMasterInfo, 
        Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker)
    {
        var rnd = new Random();

        var ids = grainIDToPartitionID.Keys.ToList();

        var grainID = ids[rnd.Next(0, ids.Count)];
        var oldPartitionID = grainIDToPartitionID[grainID];
        (var mwGuid, var mwLocation) = grainIDToMigrationWorker[grainID];

        var partitions = partitionIDToMasterInfo.Keys.ToList();
        var newPartitionID = partitions[rnd.Next(0, partitions.Count)];
        while (newPartitionID == oldPartitionID) newPartitionID = partitions[rnd.Next(0, partitions.Count)];

        return (grainID, newPartitionID, mwGuid, mwLocation);
    }
}
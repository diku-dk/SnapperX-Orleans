using Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;
using Concurrency.Common;
using Concurrency.Common.Cache;
using Experiment.Common;
using Orleans.Runtime;

namespace Experiment.Worker;

internal class ConsumerThread
{
    readonly bool isReplica;
    readonly bool isDet;
    readonly IBenchmark benchmark;
    readonly BenchmarkType benchmarkType;
    readonly int pipeSize;
    ConcurrentQueue<RequestData> queue;
    readonly Stopwatch globalWatch;
    readonly Barrier[] barriers;
    bool[] isEpochFinish;
    readonly bool[] isProducerFinish;
    readonly int epochDuration;

    List<Task<TransactionResult>> tasks;

    /// <summary> (txn start time, is-multi-silo, is-multi-region, txn type) </summary>
    Dictionary<Task<TransactionResult>, (DateTime, bool, bool, string)> reqs; 

    // the result is only for one epoch of one consumer thread
    Dictionary<string, ExtendedResult> single_silo_single_region;
    Dictionary<string, ExtendedResult> multi_silo_single_region;
    Dictionary<string, ExtendedResult> multi_silo_multi_region;

    public ConsumerThread(
        bool isReplica,
        string myRegionID,
        string mySiloID,
        string myReplicaSiloID,
        bool isMigrationExp,
        IEnvConfigure envConfigureHelper,
        StaticClusterInfo staticClusterInfo,
        StaticReplicaInfo? staticReplicaInfo,
        bool isDet,
        BenchmarkType benchmark,
        ImplementationType implementationType,
        IWorkloadConfigure workload,
        IClusterClient client,
        ConcurrentQueue<RequestData> queue,
        Barrier[] barriers,
        bool[] isEpochFinish,
        bool[] isProducerFinish)
    {
        this.isReplica = isReplica;
        this.isDet = isDet;
        this.benchmarkType = benchmark;
        epochDuration = Constants.epochDurationMSecs;
        if (isMigrationExp) epochDuration = Constants.epochDurationForMigrationExp;
        
        switch (benchmark)
        {
            case BenchmarkType.SMALLBANK:
                this.benchmark = new SmallBank.Workload.Benchmark(myRegionID, mySiloID, myReplicaSiloID, isDet, implementationType, client, envConfigureHelper, staticClusterInfo, staticReplicaInfo);
                break;
            case BenchmarkType.MARKETPLACE:
                this.benchmark = new MarketPlace.Workload.Benchmark(myRegionID, mySiloID, myReplicaSiloID, isDet, implementationType, client, envConfigureHelper, staticClusterInfo, staticReplicaInfo);
                break;
            default:
                throw new Exception($"The benchmark {benchmark} is not supported yet. ");
        }

        pipeSize = isDet ? workload.GetPactPipeSize() : workload.GetActPipeSize();
        this.queue = queue;
        globalWatch = new Stopwatch();
        this.barriers = barriers;
        this.isEpochFinish = isEpochFinish;
        this.isProducerFinish = isProducerFinish;

        single_silo_single_region = new Dictionary<string, ExtendedResult>();
        multi_silo_single_region = new Dictionary<string, ExtendedResult>();
        multi_silo_multi_region = new Dictionary<string, ExtendedResult>();
        tasks = new List<Task<TransactionResult>>();
        reqs = new Dictionary<Task<TransactionResult>, (DateTime, bool, bool, string)>();
    }

    public async Task<WorkloadResult> RunEpoch(int eIndex)
    {
        var startTime = await RunEpochForRegularExp(eIndex);
        isEpochFinish[eIndex] = true;   // which means producer doesn't need to produce more requests

        // Wait for the tasks exceeding epoch time and also count them into results
        while (tasks.Count != 0) await WaitForTxnTaskAsync();
        long endTime = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
        globalWatch.Stop();

        // generate experiment result
        var res = new WorkloadResult(benchmarkType);
        res.SetResult(isDet, single_silo_single_region, multi_silo_single_region, multi_silo_multi_region);
        res.SetTime(startTime, endTime);

        foreach (var item in single_silo_single_region)
        {
            var minTxnSize = item.Value.basic_result.txnSize.Count == 0 ? 0 : item.Value.basic_result.txnSize.Min();
            var maxTxnSize = item.Value.basic_result.txnSize.Count == 0 ? 0 : item.Value.basic_result.txnSize.Max();

            if (isDet) Console.WriteLine($"ConsumerThread: isDet = {isDet}, txn = {item.Key} (size = {minTxnSize}, {maxTxnSize}), tp_sSsR = {item.Value.basic_result.numCommit * 1000 / (endTime - startTime)}");
            else
            {
                var abort = (item.Value.numEmit - item.Value.basic_result.numCommit) * 100.0 / item.Value.numEmit; 
                Console.WriteLine($"ConsumerThread: isDet = {isDet}, txn = {item.Key} (size = {minTxnSize}, {maxTxnSize}), tp_sSsR = {item.Value.basic_result.numCommit * 1000 / (endTime - startTime)}, abort = {Helper.ChangeFormat(abort, 2)} %");
            }
        }
        foreach (var item in multi_silo_single_region)
        {
            var minTxnSize = item.Value.basic_result.txnSize.Count == 0 ? 0 : item.Value.basic_result.txnSize.Min();
            var maxTxnSize = item.Value.basic_result.txnSize.Count == 0 ? 0 : item.Value.basic_result.txnSize.Max();

            if (isDet) Console.WriteLine($"ConsumerThread: isDet = {isDet}, txn = {item.Key} (size = {minTxnSize}, {maxTxnSize}), tp_mSsR = {item.Value.basic_result.numCommit * 1000 / (endTime - startTime)}");
            else
            {
                var abort = (item.Value.numEmit - item.Value.basic_result.numCommit) * 100.0 / item.Value.numEmit;
                Console.WriteLine($"ConsumerThread: isDet = {isDet}, txn = {item.Key} (size = {minTxnSize}, {maxTxnSize}), tp_mSsR = {item.Value.basic_result.numCommit * 1000 / (endTime - startTime)}, abort = {Helper.ChangeFormat(abort, 2)} %");
            }
        } 
        foreach (var item in multi_silo_multi_region)
        {
            var minTxnSize = item.Value.basic_result.txnSize.Count == 0 ? 0 : item.Value.basic_result.txnSize.Min();
            var maxTxnSize = item.Value.basic_result.txnSize.Count == 0 ? 0 : item.Value.basic_result.txnSize.Max();

            if (isDet) Console.WriteLine($"ConsumerThread: isDet = {isDet}, txn = {item.Key} (size = {minTxnSize}, {maxTxnSize}), tp_mSmR = {item.Value.basic_result.numCommit * 1000 / (endTime - startTime)}");
            else
            {
                var abort = (item.Value.numEmit - item.Value.basic_result.numCommit) * 100.0 / item.Value.numEmit;
                Console.WriteLine($"ConsumerThread: isDet = {isDet}, txn = {item.Key} (size = {minTxnSize}, {maxTxnSize}), tp_mSmR = {item.Value.basic_result.numCommit * 1000 / (endTime - startTime)}, abort = {Helper.ChangeFormat(abort, 2)} %");
            }
        }

        return res;
    }

    /// <returns> the exact start time </returns>
    async Task<long> RunEpochForRegularExp(int eIndex)
    {
        var numEmit = 0;
        barriers[eIndex].SignalAndWait();
        globalWatch.Restart();
        var startTime = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
        RequestData? txn;
        do
        {
            while (tasks.Count < pipeSize && queue.TryDequeue(out txn))
            {
                var startTxnTime = DateTime.UtcNow;
                var newTask = benchmark.NewTransaction(txn, isReplica);
                reqs.Add(newTask, (startTxnTime, txn.isMultiSiloTxn, txn.isMultiRegionTxn, txn.transactionType));
                tasks.Add(newTask);
                numEmit++;

                if (txn.isMultiSiloTxn && txn.isMultiRegionTxn)
                {
                    if (!multi_silo_multi_region.ContainsKey(txn.transactionType)) multi_silo_multi_region.Add(txn.transactionType, new ExtendedResult());
                    multi_silo_multi_region[txn.transactionType].numEmit++;
                }
                else if (txn.isMultiSiloTxn && !txn.isMultiRegionTxn)
                {
                    if (!multi_silo_single_region.ContainsKey(txn.transactionType)) multi_silo_single_region.Add(txn.transactionType, new ExtendedResult());
                    multi_silo_single_region[txn.transactionType].numEmit++;
                }
                else
                {
                    if (!single_silo_single_region.ContainsKey(txn.transactionType)) single_silo_single_region.Add(txn.transactionType, new ExtendedResult());
                    single_silo_single_region[txn.transactionType].numEmit++;
                }
            }
            if (tasks.Count != 0) await WaitForTxnTaskAsync();
        }
        while (globalWatch.ElapsedMilliseconds < epochDuration && (queue.Count != 0 || !isProducerFinish[eIndex]));
        if (numEmit != 0) Console.WriteLine($"ConsumerThread: exists after running {globalWatch.ElapsedMilliseconds}ms, numEmit = {numEmit}");
        
        return startTime;
    }

    async Task WaitForTxnTaskAsync()
    {
        var orleansTxnAbort = false;
        var task = await Task.WhenAny(tasks);
        var endTxnTime = DateTime.UtcNow;
        var startTxnTime = reqs[task].Item1;
        var isMultiSiloTxn = reqs[task].Item2;
        var isMultiRegionTxn = reqs[task].Item3;
        var transactionType = reqs[task].Item4;
        try
        {
            // Needed to catch exception of individual task (not caught by Snapper's exception) which would not be thrown by WhenAny
            await task;
        }
        catch (OrleansException)   // this exception is only related to OrleansTransaction
        {
            //Console.WriteLine($"{e.Message}, {e.StackTrace}");
            orleansTxnAbort = true;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Unexpected exception: {e.GetType}, {e.Message} {e.StackTrace}");
            Debug.Assert(false);
        }

        if (!orleansTxnAbort && !task.Result.hasException())
        {
            // check grains accessed by the transaction
            var txnSize = task.Result.grains.Count;
            var locations = task.Result.grains.Select(x => x.GetLocation()).ToList();
            var isMultiRegion = locations.Select(x => x.Item1).ToHashSet().Count > 1;
            var isMultiSilo = locations.Select(x => x.Item2).ToHashSet().Count > 1;

            //if (txnSize == 5) Console.WriteLine($"grains = {string.Join(", ", task.Result.grains.Select(x => x.grainID.Print()))}");

            // if grain is being migrated, or if the grain has references registered, the previous info are not valid anymore
            /*
            if (benchmarkType == BenchmarkType.SMALLBANK && !isMigrationExp)
            {
                if (!isMultiRegionTxn.Equals(isMultiRegion) || !isMultiSiloTxn.Equals(isMultiSilo))
                    Console.WriteLine($"acccessed grains: {string.Join(", ", task.Result.grains.Keys.Select(x => x.grainID.Print() + " || " + x.location))}");
                Debug.Assert(isMultiRegionTxn.Equals(isMultiRegion) && isMultiSiloTxn.Equals(isMultiSilo));
            }
            */

            // re-consider isMultiRegion and isMultiSilo
            if (isMultiRegion != isMultiRegionTxn || isMultiSilo != isMultiSiloTxn)
            {
                // reduce the previous added number
                if (isMultiSiloTxn)
                {
                    if (isMultiRegionTxn) multi_silo_multi_region[transactionType].numEmit--;
                    else multi_silo_single_region[transactionType].numEmit--;
                }
                else single_silo_single_region[transactionType].numEmit--;

                // add the new number to the correct category
                isMultiSiloTxn = isMultiSilo;
                isMultiRegionTxn = isMultiRegion;

                if (isMultiSiloTxn)
                {
                    if (isMultiRegionTxn)
                    {
                        if (!multi_silo_multi_region.ContainsKey(transactionType)) multi_silo_multi_region.Add(transactionType, new ExtendedResult());
                        multi_silo_multi_region[transactionType].numEmit++;
                    }
                    else
                    {
                        if (!multi_silo_single_region.ContainsKey(transactionType)) multi_silo_single_region.Add(transactionType, new ExtendedResult());
                        multi_silo_single_region[transactionType].numEmit++;
                    }
                }
                else
                {
                    if (!single_silo_single_region.ContainsKey(transactionType)) single_silo_single_region.Add(transactionType, new ExtendedResult());
                    single_silo_single_region[transactionType].numEmit++;
                }
            }

            if (isMultiSiloTxn)
            {
                if (isMultiRegionTxn)
                {
                    multi_silo_multi_region[transactionType].basic_result.numCommit++;
                    multi_silo_multi_region[transactionType].basic_result.txnSize.Add(txnSize);
                    multi_silo_multi_region[transactionType].basic_result.latencies.latency.Add((endTxnTime - startTxnTime).TotalMilliseconds);
                    foreach (var time in task.Result.time) multi_silo_multi_region[transactionType].basic_result.latencies.breakdown[time.Key].Add(time.Value);
                }
                else
                {
                    multi_silo_single_region[transactionType].basic_result.numCommit++;
                    multi_silo_single_region[transactionType].basic_result.txnSize.Add(txnSize);
                    multi_silo_single_region[transactionType].basic_result.latencies.latency.Add((endTxnTime - startTxnTime).TotalMilliseconds);
                    foreach (var time in task.Result.time) multi_silo_single_region[transactionType].basic_result.latencies.breakdown[time.Key].Add(time.Value);
                    //task.Result.Print();
                }
            }
            else
            {
                single_silo_single_region[transactionType].basic_result.numCommit++;
                single_silo_single_region[transactionType].basic_result.txnSize.Add(txnSize);
                single_silo_single_region[transactionType].basic_result.latencies.latency.Add((endTxnTime - startTxnTime).TotalMilliseconds);
                foreach (var time in task.Result.time) single_silo_single_region[transactionType].basic_result.latencies.breakdown[time.Key].Add(time.Value);
            }
        }
        else
        {
            var exception = orleansTxnAbort ? ExceptionType.RWConflict : task.Result.exception;

            if (isMultiSiloTxn)
            {
                if (isMultiRegionTxn) multi_silo_multi_region[transactionType].SetAbortReasons(exception);
                else multi_silo_single_region[transactionType].SetAbortReasons(exception);
            }
            else single_silo_single_region[transactionType].SetAbortReasons(exception);
        }

        tasks.Remove(task);
        reqs.Remove(task);
    }
}
using Experiment.Common;
using MathNet.Numerics.Statistics;
using System.Diagnostics;
using Utilities;

namespace Experiment.Worker;

public class ExperimentResultAggregator
{
    readonly bool isReplica;
    readonly int numResultPerEpoch;
    readonly int[] percentilesToCalculate;
    readonly IEnvSetting envSetting;
    readonly ImplementationType implementationType;
    readonly IWorkloadConfigure workload;
    readonly BenchmarkType benchmark;

    readonly int numEpoch;
    readonly int numWarmupEpoch;

    /// <summary> epoch ID, thread ID / silo ID </summary>
    WorkloadResult[,] results;  

    public ExperimentResultAggregator(bool isReplica, IEnvSetting envSetting, ImplementationType implementationType, int numEpoch, int numResultPerEpoch, IWorkloadConfigure workload, BenchmarkType benchmark)
    {
        this.isReplica = isReplica;
        this.envSetting = envSetting;
        this.implementationType = implementationType;
        this.numResultPerEpoch = numResultPerEpoch;
        this.workload = workload;
        this.benchmark = benchmark;
        percentilesToCalculate = [50, 90, 99];
        results = new WorkloadResult[numEpoch, numResultPerEpoch];

        this.numEpoch = numEpoch;
        numWarmupEpoch = numEpoch == 1 ? 0 : Constants.numWarmupEpoch;
    }

    public void SetResult(int epoch, int index, WorkloadResult result) => results[epoch, index] = result;
 
    public static WorkloadResult AggregateResultForEpoch(WorkloadResult[] result)
    {
        var aggResult = result[0];
        for (int i = 1; i < result.Length; i++) aggResult.MergeData(result[i]);
        return aggResult;
    }

    public bool AggregateResultsAndPrint(int numRun, int numReRun)
    {
        var printData = new PrintData(benchmark);
        for (int e = numWarmupEpoch; e < numEpoch; e++) 
        {
            var result = new WorkloadResult[numResultPerEpoch];
            for (int i = 0; i < result.Length; i++) result[i] = results[e, i];
            var aggResultForOneEpoch = AggregateResultForEpoch(result);
            printData.CalculateAndAdd(aggResultForOneEpoch);
        }
        
        if (implementationType != ImplementationType.ORLEANSTXN && numRun < numReRun - 1)
        {
            var pass = true;

            foreach (var item in printData.pact_single_silo_single_region_print) pass &= CheckSdSafeRange(item.Value.throughput);
            foreach (var item in printData.pact_multi_silo_single_region_print) pass &= CheckSdSafeRange(item.Value.throughput);
            foreach (var item in printData.pact_multi_silo_multi_region_print) pass &= CheckSdSafeRange(item.Value.throughput);

            foreach (var item in printData.act_single_silo_single_region_print) pass &= CheckSdSafeRange(item.Value.basic_data.throughput);
            foreach (var item in printData.act_multi_silo_single_region_print) pass &= CheckSdSafeRange(item.Value.basic_data.throughput);
            foreach (var item in printData.act_multi_silo_multi_region_print) pass &= CheckSdSafeRange(item.Value.basic_data.throughput);

            if (!pass) return false;
        }

        var basicEnvSetting = envSetting.GetBasic();
        var fileName = isReplica ? Constants.replicaResultPath : Constants.resultPath;
        using (var file = new StreamWriter(fileName, true))
        {
            if (basicEnvSetting.benchmark == BenchmarkType.MARKETPLACE)
            {
                var txns = new List<MarketPlaceTxnType>
                {
                    MarketPlaceTxnType.AddItemToCart,
                    MarketPlaceTxnType.DeleteItemInCart,
                    MarketPlaceTxnType.Checkout,
                    MarketPlaceTxnType.UpdatePrice
                };

                var marketplaceEnv = envSetting as MarketPlace.Workload.EnvSetting;
                Debug.Assert(marketplaceEnv != null);
                var marketplaceWorkload = workload as MarketPlace.Workload.WorkloadConfigure;
                Debug.Assert(marketplaceWorkload != null);

                // environment setting
                file.Write($"{basicEnvSetting.implementationType} {basicEnvSetting.numSiloPerRegion} {basicEnvSetting.doLogging} ");
                file.Write($"{marketplaceEnv.numCustomerPerCity} {marketplaceEnv.numSellerPerCity} {marketplaceEnv.numProductPerSeller} ");

                // workload setting
                file.Write($"{Helper.ChangeFormat(marketplaceWorkload.hotCustomerPerCity, 1)}% ");
                file.Write($"{Helper.ChangeFormat(marketplaceWorkload.hotSellerPerCity, 1)}% ");
                file.Write($"{Helper.ChangeFormat(marketplaceWorkload.hotProductPerSeller, 1)}% ");
                
                file.Write($"{marketplaceWorkload.pactPercent}% {marketplaceWorkload.pactPipeSize} {marketplaceWorkload.actPipeSize} {marketplaceWorkload.numPactConsumer} ");

                // txn percentage
                foreach (var txn in txns) file.Write($"{marketplaceWorkload.txnTypes[txn]}% ");

                // total throughput
                foreach (var txn in txns)
                {
                    var pact_non_dist = printData.pact_single_silo_single_region_print[txn.ToString()].throughput.Mean();
                    var act_non_dist = printData.act_single_silo_single_region_print[txn.ToString()].basic_data.throughput.Mean();
                    var pact_dist = printData.pact_multi_silo_single_region_print[txn.ToString()].throughput.Mean();
                    var act_dist = printData.act_multi_silo_single_region_print[txn.ToString()].basic_data.throughput.Mean();
                    file.Write($"{Helper.ChangeFormat(pact_non_dist + act_non_dist + pact_dist + act_dist, 0)} ");
                }

                foreach (var txn in txns)
                {
                    file.Write($"{txn} ");

                    var pact_non_dist = printData.pact_single_silo_single_region_print[txn.ToString()].throughput.Mean();
                    var act_non_dist = printData.act_single_silo_single_region_print[txn.ToString()].basic_data.throughput.Mean();
                    var pact_dist = printData.pact_multi_silo_single_region_print[txn.ToString()].throughput.Mean();
                    var act_dist = printData.act_multi_silo_single_region_print[txn.ToString()].basic_data.throughput.Mean();

                    // pact throughput
                    file.Write($"{Helper.ChangeFormat(pact_non_dist, 0)} ");
                    //file.Write($"{Helper.ChangeFormat(printData.pact_single_silo_single_region_print[txn.ToString()].GetAverageTxnSize(), 0)} ");
                    file.Write($"{Helper.ChangeFormat(pact_dist, 0)} ");
                    //file.Write($"{Helper.ChangeFormat(printData.pact_multi_silo_single_region_print[txn.ToString()].GetAverageTxnSize(), 0)} ");

                    // act throughput
                    file.Write($"{Helper.ChangeFormat(act_non_dist, 0)} ");
                    //file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn.ToString()].basic_data.GetAverageTxnSize(), 0)} ");
                    file.Write($"{Helper.ChangeFormat(act_dist, 0)} ");
                    //file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn.ToString()].basic_data.GetAverageTxnSize(), 0)} ");

                    // non-dist abort
                    file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn.ToString()].abort.Mean(), 2)}% ");
                    file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn.ToString()].abortReasons[ExceptionType.RWConflict].Mean(), 2)}% ");
                    file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn.ToString()].abortReasons[ExceptionType.NotSerializable].Mean(), 2)}% ");
                    file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn.ToString()].abortReasons[ExceptionType.AppAbort].Mean(), 2)}% ");

                    // dist abort
                    file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn.ToString()].abort.Mean(), 2)}% ");
                    file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn.ToString()].abortReasons[ExceptionType.RWConflict].Mean(), 2)}% ");
                    file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn.ToString()].abortReasons[ExceptionType.NotSerializable].Mean(), 2)}% ");
                    file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn.ToString()].abortReasons[ExceptionType.AppAbort].Mean(), 2)}% ");

                    if (txn == MarketPlaceTxnType.UpdatePrice)
                    {
                        // non-dist
                        file.Write($"dist ");
                        var histogram = new SortedDictionary<int, int>();
                        foreach (var item in printData.act_single_silo_single_region_print[txn.ToString()].basic_data.txnSize)
                        {
                            if (!histogram.ContainsKey(item)) histogram.Add(item, 0);
                            else histogram[item]++;
                        }

                        foreach (var item in histogram) file.Write($"{item.Key}-{item.Value} ");

                        // dist
                        file.Write($"non-dist ");
                        histogram = new SortedDictionary<int, int>();
                        foreach (var item in printData.act_multi_silo_single_region_print[txn.ToString()].basic_data.txnSize)
                        {
                            if (!histogram.ContainsKey(item)) histogram.Add(item, 0);
                            else histogram[item]++;
                        }

                        foreach (var item in histogram) file.Write($"{item.Key}-{item.Value} ");
                    }
                }

                file.WriteLine();
            }
            else if (basicEnvSetting.benchmark == BenchmarkType.SMALLBANK)
            {
                var txn = SmallBankTxnType.MultiTransfer.ToString();

                var smallBankEnv = envSetting as SmallBank.Workload.EnvSetting;
                Debug.Assert(smallBankEnv != null);
                var smallBankWorkload = workload as SmallBank.Workload.WorkloadConfigure;
                Debug.Assert(smallBankWorkload != null);

                // environment setting
                file.Write($"{basicEnvSetting.implementationType} {basicEnvSetting.numSiloPerRegion} {basicEnvSetting.doLogging} {smallBankEnv.numGrainPerPartition} {smallBankEnv.numAccountPerGrain} ");
                file.Write($"{basicEnvSetting.regionalBatchSizeInMSecs}ms {basicEnvSetting.speculativeBatch} ");

                // workload setting
                file.Write($"{smallBankWorkload.multiSiloPercent}% {smallBankWorkload.numGrainPerTxn} {smallBankWorkload.numKeyToAccessPerGrain} ");
                file.Write($"{smallBankWorkload.readKeyPercent}% {smallBankWorkload.hotKeyPercent}% {smallBankWorkload.hotPercentPerGrain}% {smallBankWorkload.hotGrainPercent}% {smallBankWorkload.hotPercentPerPartition}% ");
                file.Write($"{smallBankWorkload.pactPercent}% {smallBankWorkload.pactPipeSize} {smallBankWorkload.actPipeSize} {smallBankWorkload.numPactConsumer} ");

                // total throughput
                var total = printData.pact_single_silo_single_region_print[txn].throughput.Mean() +
                    printData.pact_multi_silo_single_region_print[txn].throughput.Mean() +
                    printData.act_single_silo_single_region_print[txn].basic_data.throughput.Mean() +
                    printData.act_multi_silo_single_region_print[txn].basic_data.throughput.Mean();
                file.Write($"{Helper.ChangeFormat(total, 0)} ");

                // throughput
                file.Write($"{Helper.ChangeFormat(printData.pact_single_silo_single_region_print[txn].throughput.Mean(), 0)} " +
                           $"{Helper.ChangeFormat(printData.pact_multi_silo_single_region_print[txn].throughput.Mean(), 0)} " +

                           $"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].basic_data.throughput.Mean(), 0)} " +
                           $"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].basic_data.throughput.Mean(), 0)} ");

                // ACT total abort rate
                file.Write($"{Helper.ChangeFormat(printData.act_total_abort[txn].Mean(), 2)}% ");

                // abort reasons
                file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].abort.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].abortReasons[ExceptionType.RWConflict].Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].abortReasons[ExceptionType.NotSerializable].Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].abortReasons[ExceptionType.AppAbort].Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].abortReasons[ExceptionType.GrainMigration].Mean(), 2)}% ");

                file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].abort.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].abortReasons[ExceptionType.RWConflict].Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].abortReasons[ExceptionType.NotSerializable].Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].abortReasons[ExceptionType.AppAbort].Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].abortReasons[ExceptionType.GrainMigration].Mean(), 2)}% ");

                // PACT percentile latencies
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(printData.pact_single_silo_single_region_print[txn].latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 2)} ");
                }
                foreach (BreakDownLatency time in Enum.GetValues(typeof(BreakDownLatency)))
                    file.Write($"{Helper.ChangeFormat(printData.pact_single_silo_single_region_print[txn].breakdown[time].ToArray().Mean(), 2)} ");
                
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(printData.pact_multi_silo_single_region_print[txn].latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 2)} ");
                }
                foreach (BreakDownLatency time in Enum.GetValues(typeof(BreakDownLatency)))
                    file.Write($"{Helper.ChangeFormat(printData.pact_multi_silo_single_region_print[txn].breakdown[time].ToArray().Mean(), 2)} ");
                
                // ACT percentile latencies
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(printData.act_single_silo_single_region_print[txn].basic_data.latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 2)} ");
                }
                foreach (BreakDownLatency time in Enum.GetValues(typeof(BreakDownLatency)))
                    file.Write($"{Helper.ChangeFormat(printData.act_single_silo_single_region_print[txn].basic_data.breakdown[time].ToArray().Mean(), 2)} ");
                
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(printData.act_multi_silo_single_region_print[txn].basic_data.latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 2)} ");
                }
                foreach(BreakDownLatency time in Enum.GetValues(typeof(BreakDownLatency)))
                    file.Write($"{Helper.ChangeFormat(printData.act_multi_silo_single_region_print[txn].basic_data.breakdown[time].ToArray().Mean(), 2)} ");
                
                file.WriteLine();
            }
        }
        return true;
    }

    bool CheckSdSafeRange(List<double> data)
    { 
        if (data.Count != 0) return data.StandardDeviation() <= data.Mean() * Constants.sdSafeRange;
        return true;
    } 
}
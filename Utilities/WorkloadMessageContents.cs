using MessagePack;
using System.Diagnostics;

namespace Utilities;

[MessagePackObject]
public class BasicLatencyInfo
{
    [Key(0)]
    public List<double> latency;

    [Key(1)]
    public Dictionary<BreakDownLatency, List<double>> breakdown;

    public BasicLatencyInfo()
    {
        latency = new List<double>();
        breakdown = new Dictionary<BreakDownLatency, List<double>>();
        foreach (BreakDownLatency time in Enum.GetValues(typeof(BreakDownLatency))) breakdown.Add(time, new List<double>());
    }

    public void MergeData(BasicLatencyInfo latencies)
    {
        latency.AddRange(latencies.latency);
        foreach (var item in latencies.breakdown) breakdown[item.Key].AddRange(item.Value);
    }
}

[MessagePackObject]
public class BasicResult
{
    [Key(0)]
    public int numCommit;

    [Key(1)]
    public BasicLatencyInfo latencies;

    [Key(2)]
    public List<int> txnSize;

    public BasicResult()
    {
        numCommit = 0;
        latencies = new BasicLatencyInfo();
        txnSize = new List<int>();
    }

    public void MergeData(BasicResult res)
    {
        numCommit += res.numCommit;
        latencies.MergeData(res.latencies);
        txnSize.AddRange(res.txnSize);
    }
}

[MessagePackObject]
public class ExtendedResult
{
    [Key(0)]
    public BasicResult basic_result;
    [Key(1)]
    public int numEmit;
    [Key(2)]
    public Dictionary<ExceptionType, int> abortReasons;

    public ExtendedResult()
    {
        basic_result = new BasicResult();
        numEmit = 0;

        abortReasons = new Dictionary<ExceptionType, int>();
        foreach (ExceptionType exp in Enum.GetValues(typeof(ExceptionType))) abortReasons.Add(exp, 0);
    } 
    
    public void MergeData(ExtendedResult res)
    {
        basic_result.MergeData(res.basic_result);
        numEmit += res.numEmit;
        foreach (var item in res.abortReasons)
        {
            var oldCount = abortReasons[item.Key];
            abortReasons[item.Key] = oldCount + item.Value;
        }
    }

    public void SetAbortReasons(ExceptionType? exception)
    {
        if (exception == null) return;

        var oldCount = abortReasons[exception.Value];
        abortReasons[exception.Value] = oldCount + 1;
    }
}

[MessagePackObject]
public class WorkloadResult
{
    [Key(0)]
    public long startTime;
    [Key(1)]
    public long endTime;

    [Key(2)]
    public Dictionary<string, BasicResult> pact_single_silo_single_region;
    [Key(3)]
    public Dictionary<string, BasicResult> pact_multi_silo_single_region;
    [Key(4)]
    public Dictionary<string, BasicResult> pact_multi_silo_multi_region;

    [Key(5)]
    public Dictionary<string, ExtendedResult> act_single_silo_single_region;
    [Key(6)]
    public Dictionary<string, ExtendedResult> act_multi_silo_single_region;
    [Key(7)]
    public Dictionary<string, ExtendedResult> act_multi_silo_multi_region;

    public WorkloadResult() { }

    public WorkloadResult(BenchmarkType benchmark)
    {
        startTime = 0;
        endTime = 0;
        pact_single_silo_single_region = new Dictionary<string, BasicResult>();
        pact_multi_silo_single_region = new Dictionary<string, BasicResult>();
        pact_multi_silo_multi_region = new Dictionary<string, BasicResult>();
        act_single_silo_single_region = new Dictionary<string, ExtendedResult>();
        act_multi_silo_single_region = new Dictionary<string, ExtendedResult>();
        act_multi_silo_multi_region = new Dictionary<string, ExtendedResult>();

        switch (benchmark)
        {
            case BenchmarkType.SMALLBANK:
                foreach (SmallBankTxnType type in Enum.GetValues(typeof(SmallBankTxnType)))
                {
                    pact_single_silo_single_region.Add(type.ToString(), new BasicResult());
                    pact_multi_silo_single_region.Add(type.ToString(), new BasicResult());
                    pact_multi_silo_multi_region.Add(type.ToString(), new BasicResult());
                    act_single_silo_single_region.Add(type.ToString(), new ExtendedResult());
                    act_multi_silo_single_region.Add(type.ToString(), new ExtendedResult());
                    act_multi_silo_multi_region.Add(type.ToString(), new ExtendedResult());
                }
                break;
            case BenchmarkType.MARKETPLACE:
                foreach (MarketPlaceTxnType type in Enum.GetValues(typeof(MarketPlaceTxnType)))
                {
                    pact_single_silo_single_region.Add(type.ToString(), new BasicResult());
                    pact_multi_silo_single_region.Add(type.ToString(), new BasicResult());
                    pact_multi_silo_multi_region.Add(type.ToString(), new BasicResult());
                    act_single_silo_single_region.Add(type.ToString(), new ExtendedResult());
                    act_multi_silo_single_region.Add(type.ToString(), new ExtendedResult());
                    act_multi_silo_multi_region.Add(type.ToString(), new ExtendedResult());
                }
                break;
            default:
                throw new Exception($"The benchmark {benchmark} is not supported. ");
        }
    }

    public void SetResult(
        bool isDet, 
        Dictionary<string, ExtendedResult> single_silo_single_region,
        Dictionary<string, ExtendedResult> multi_silo_single_region,
        Dictionary<string, ExtendedResult> multi_silo_multi_region)
    {
        if (isDet)
        {
            foreach (var item in single_silo_single_region)
            {
                Debug.Assert(pact_single_silo_single_region.ContainsKey(item.Key)); 
                pact_single_silo_single_region[item.Key] = item.Value.basic_result;
            }
            foreach (var item in multi_silo_single_region)
            {
                Debug.Assert(pact_multi_silo_single_region.ContainsKey(item.Key));
                pact_multi_silo_single_region[item.Key] = item.Value.basic_result;
            }
            foreach (var item in multi_silo_multi_region)
            {
                Debug.Assert(pact_multi_silo_multi_region.ContainsKey(item.Key));
                pact_multi_silo_multi_region[item.Key] = item.Value.basic_result;
            }
        }
        else 
        {
            foreach (var item in single_silo_single_region)
            {
                Debug.Assert(act_single_silo_single_region.ContainsKey(item.Key));
                act_single_silo_single_region[item.Key] = item.Value;
            }
            foreach (var item in multi_silo_single_region)
            {
                Debug.Assert(act_multi_silo_single_region.ContainsKey(item.Key));
                act_multi_silo_single_region[item.Key] = item.Value;
            }
            foreach (var item in multi_silo_multi_region)
            {
                Debug.Assert(act_multi_silo_multi_region.ContainsKey(item.Key));
                act_multi_silo_multi_region[item.Key] = item.Value;
            }
        }
    }

    public void SetTime(long startTime, long endTime)
    {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public void MergeData(WorkloadResult res)
    {
        startTime = Math.Min(startTime, res.startTime);
        endTime = Math.Max(endTime, res.endTime);

        foreach (var item in res.pact_single_silo_single_region)
        {
            Debug.Assert(pact_single_silo_single_region.ContainsKey(item.Key));
            pact_single_silo_single_region[item.Key].MergeData(item.Value);
        }
        foreach (var item in res.pact_multi_silo_single_region)
        {
            Debug.Assert(pact_multi_silo_single_region.ContainsKey(item.Key));
            pact_multi_silo_single_region[item.Key].MergeData(item.Value);
        }
        foreach (var item in res.pact_multi_silo_multi_region)
        {
            Debug.Assert(pact_multi_silo_multi_region.ContainsKey(item.Key));
            pact_multi_silo_multi_region[item.Key].MergeData(item.Value);
        }

        foreach (var item in res.act_single_silo_single_region)
        {
            Debug.Assert(act_single_silo_single_region.ContainsKey(item.Key));
            act_single_silo_single_region[item.Key].MergeData(item.Value);
        }
        foreach (var item in res.act_multi_silo_single_region)
        {
            Debug.Assert(act_multi_silo_single_region.ContainsKey(item.Key));
            act_multi_silo_single_region[item.Key].MergeData(item.Value);
        }
        foreach (var item in res.act_multi_silo_multi_region)
        {
            Debug.Assert(act_multi_silo_multi_region.ContainsKey(item.Key));
            act_multi_silo_multi_region[item.Key].MergeData(item.Value);
        }
    }
}

public class BasicExperimentResult
{
    public List<double> throughput;

    public List<int> txnSize;
    
    public List<double> latency;

    public Dictionary<BreakDownLatency, List<double>> breakdown;

    public BasicExperimentResult()
    {
        throughput = new List<double>();
        latency = new List<double>();
        txnSize = new List<int>();
        breakdown = new Dictionary<BreakDownLatency, List<double>>();
        foreach (BreakDownLatency time in Enum.GetValues(typeof(BreakDownLatency))) breakdown.Add(time, new List<double>());
    }

    public void CalculateAndAdd(int numCommit, long startTime, long endTime, BasicLatencyInfo latencies, List<int> txnSize)
    {
        this.txnSize.AddRange(txnSize);

        throughput.Add(numCommit * 1000.0 / (endTime - startTime));
        latency.AddRange(latencies.latency);

        foreach (var item in latencies.breakdown) breakdown[item.Key].AddRange(item.Value);
    }

    public double GetAverageTxnSize() => txnSize.Count == 0 ? Double.NaN : txnSize.Average();
}

public class ExtendedExperimentResult
{
    public BasicExperimentResult basic_data;
    public List<double> abort;
    public Dictionary<ExceptionType, List<double>> abortReasons;

    public ExtendedExperimentResult()
    {
        basic_data = new BasicExperimentResult();
        abort = new List<double>();

        abortReasons = new Dictionary<ExceptionType, List<double>>();
        foreach (ExceptionType exp in Enum.GetValues(typeof(ExceptionType))) abortReasons.Add(exp, new List<double>());
    }

    public void CalculateAndAdd(int numCommit, long startTime, long endTime, int numEmit, Dictionary<ExceptionType, int> abortReasons, BasicLatencyInfo latencies, List<int> txnSize)
    {
        basic_data.CalculateAndAdd(numCommit, startTime, endTime, latencies, txnSize);
        var numAbort = numEmit - numCommit;
        abort.Add(numAbort * 100.0 / numEmit);
        if (numAbort == 0) return;

        foreach (var item in abortReasons) this.abortReasons[item.Key].Add(item.Value * 100.0 / numAbort);
    }
}

public class PrintData
{
    public Dictionary<string, BasicExperimentResult> pact_multi_silo_multi_region_print;
    public Dictionary<string, BasicExperimentResult> pact_multi_silo_single_region_print;
    public Dictionary<string, BasicExperimentResult> pact_single_silo_single_region_print;

    public Dictionary<string, List<double>> act_total_abort;
    public Dictionary<string, ExtendedExperimentResult> act_multi_silo_multi_region_print;
    public Dictionary<string, ExtendedExperimentResult> act_multi_silo_single_region_print;
    public Dictionary<string, ExtendedExperimentResult> act_single_silo_single_region_print;

    public PrintData(BenchmarkType benchmark)
    {
        act_total_abort = new Dictionary<string, List<double>>();
        pact_multi_silo_multi_region_print = new Dictionary<string, BasicExperimentResult>();
        pact_multi_silo_single_region_print = new Dictionary<string, BasicExperimentResult>();
        pact_single_silo_single_region_print = new Dictionary<string, BasicExperimentResult>();
        act_multi_silo_multi_region_print = new Dictionary<string, ExtendedExperimentResult>();
        act_multi_silo_single_region_print = new Dictionary<string, ExtendedExperimentResult>();
        act_single_silo_single_region_print = new Dictionary<string, ExtendedExperimentResult>();

        switch (benchmark)
        {
            case BenchmarkType.SMALLBANK:
                foreach (SmallBankTxnType type in Enum.GetValues(typeof(SmallBankTxnType)))
                {
                    act_total_abort.Add(type.ToString(), new List<double>());
                    pact_multi_silo_multi_region_print.Add(type.ToString(), new BasicExperimentResult());
                    pact_multi_silo_single_region_print.Add(type.ToString(), new BasicExperimentResult());
                    pact_single_silo_single_region_print.Add(type.ToString(), new BasicExperimentResult());
                    act_multi_silo_multi_region_print.Add(type.ToString(), new ExtendedExperimentResult());
                    act_multi_silo_single_region_print.Add(type.ToString(), new ExtendedExperimentResult());
                    act_single_silo_single_region_print.Add(type.ToString(), new ExtendedExperimentResult());
                }
                break;
            case BenchmarkType.MARKETPLACE:
                foreach (MarketPlaceTxnType type in Enum.GetValues(typeof(MarketPlaceTxnType)))
                {
                    act_total_abort.Add(type.ToString(), new List<double>());
                    pact_multi_silo_multi_region_print.Add(type.ToString(), new BasicExperimentResult());
                    pact_multi_silo_single_region_print.Add(type.ToString(), new BasicExperimentResult());
                    pact_single_silo_single_region_print.Add(type.ToString(), new BasicExperimentResult());
                    act_multi_silo_multi_region_print.Add(type.ToString(), new ExtendedExperimentResult());
                    act_multi_silo_single_region_print.Add(type.ToString(), new ExtendedExperimentResult());
                    act_single_silo_single_region_print.Add(type.ToString(), new ExtendedExperimentResult());
                }
                break;
            default:
                throw new Exception($"The benchmark {benchmark} is not supported. ");
        }
    }

    public void CalculateAndAdd(WorkloadResult res)
    {
        foreach (var item in res.pact_single_silo_single_region)
        {
            Debug.Assert(pact_single_silo_single_region_print.ContainsKey(item.Key));
            pact_single_silo_single_region_print[item.Key].CalculateAndAdd(item.Value.numCommit, res.startTime, res.endTime, item.Value.latencies, item.Value.txnSize);
        }
        foreach (var item in res.pact_multi_silo_single_region)
        {
            Debug.Assert(pact_multi_silo_single_region_print.ContainsKey(item.Key));
            pact_multi_silo_single_region_print[item.Key].CalculateAndAdd(item.Value.numCommit, res.startTime, res.endTime, item.Value.latencies, item.Value.txnSize);
        }
        foreach (var item in res.pact_multi_silo_multi_region)
        {
            Debug.Assert(pact_multi_silo_multi_region_print.ContainsKey(item.Key));
            pact_multi_silo_multi_region_print[item.Key].CalculateAndAdd(item.Value.numCommit, res.startTime, res.endTime, item.Value.latencies, item.Value.txnSize);
        }

        foreach (var item in res.act_single_silo_single_region)
        {
            Debug.Assert(act_single_silo_single_region_print.ContainsKey(item.Key));
            act_single_silo_single_region_print[item.Key].CalculateAndAdd(item.Value.basic_result.numCommit, res.startTime, res.endTime, item.Value.numEmit, item.Value.abortReasons, item.Value.basic_result.latencies, item.Value.basic_result.txnSize);
        }
        foreach (var item in res.act_multi_silo_single_region)
        {
            Debug.Assert(act_multi_silo_single_region_print.ContainsKey(item.Key));
            act_multi_silo_single_region_print[item.Key].CalculateAndAdd(item.Value.basic_result.numCommit, res.startTime, res.endTime, item.Value.numEmit, item.Value.abortReasons, item.Value.basic_result.latencies, item.Value.basic_result.txnSize);
        }
        foreach (var item in res.act_multi_silo_multi_region)
        {
            Debug.Assert(act_multi_silo_multi_region_print.ContainsKey(item.Key));
            act_multi_silo_multi_region_print[item.Key].CalculateAndAdd(item.Value.basic_result.numCommit, res.startTime, res.endTime, item.Value.numEmit, item.Value.abortReasons, item.Value.basic_result.latencies, item.Value.basic_result.txnSize);
        }

        var totalNumEmit = new Dictionary<string, MyCounter>();
        var totalNumCommit = new Dictionary<string, MyCounter>();
        foreach (var item in res.act_single_silo_single_region)
        {
            if (!totalNumEmit.ContainsKey(item.Key)) totalNumEmit.Add(item.Key, new MyCounter());
            if (!totalNumCommit.ContainsKey(item.Key)) totalNumCommit.Add(item.Key, new MyCounter());

            totalNumEmit[item.Key].Add(item.Value.numEmit);
            totalNumCommit[item.Key].Add(item.Value.basic_result.numCommit);
        }
        foreach (var item in res.act_multi_silo_single_region)
        {
            if (!totalNumEmit.ContainsKey(item.Key)) totalNumEmit.Add(item.Key, new MyCounter());
            if (!totalNumCommit.ContainsKey(item.Key)) totalNumCommit.Add(item.Key, new MyCounter());

            totalNumEmit[item.Key].Add(item.Value.numEmit);
            totalNumCommit[item.Key].Add(item.Value.basic_result.numCommit);
        }
        foreach (var item in res.act_multi_silo_multi_region)
        {
            if (!totalNumEmit.ContainsKey(item.Key)) totalNumEmit.Add(item.Key, new MyCounter());
            if (!totalNumCommit.ContainsKey(item.Key)) totalNumCommit.Add(item.Key, new MyCounter());

            totalNumEmit[item.Key].Add(item.Value.numEmit);
            totalNumCommit[item.Key].Add(item.Value.basic_result.numCommit);
        }

        foreach (var item in totalNumEmit)
        {
            if (!act_total_abort.ContainsKey(item.Key)) act_total_abort.Add(item.Key, new List<double>());
            Debug.Assert(totalNumCommit.ContainsKey(item.Key));

            var numEmit = totalNumEmit[item.Key].Get();
            var numCommit = totalNumCommit[item.Key].Get();
            act_total_abort[item.Key].Add((numEmit - numCommit) * 100.0 / numEmit);
        }
    }
}
using MathNet.Numerics.Statistics;
using MessagePack;
using Utilities;

namespace Concurrency.Common;

[MessagePackObject]
[GenerateSerializer]
public class AggregatedExperimentData
{
    [Id(0)]
    [Key(0)]
    public Dictionary<MonitorTime, double> times;

    public AggregatedExperimentData() => times = new Dictionary<MonitorTime, double> ();

    public void Print()
    {
        foreach (MonitorTime time in Enum.GetValues(typeof(MonitorTime)))
        {
            if (times.ContainsKey(time)) Console.WriteLine($"{time}: {Helper.ChangeFormat(times[time], 2)} ms");
            else Console.WriteLine($"{time}: NaN ms");
        }     
    }
}

[MessagePackObject]
[GenerateSerializer]
public class ExperimentData
{
    [Id(0)]
    [Key(0)]
    public Dictionary<MonitorTime, List<double>> times;

    public ExperimentData() => Clear();

    public void Set(MonitorTime type, double time)
    {
        if (time > 0) times[type].Add(time);
    }

    public void Set(AggregatedExperimentData data)
    {
        foreach (var item in data.times)
            if (item.Value > 0) times[item.Key].Add(item.Value);
    }

    public AggregatedExperimentData AggregateAndClear()
    {
        var result = new AggregatedExperimentData();
        foreach (var item in times) result.times.Add(item.Key, item.Value.Mean());

        Clear();

        return result;
    }

    void Clear()
    {
        times = new Dictionary<MonitorTime, List<double>>();
        foreach (MonitorTime time in Enum.GetValues(typeof(MonitorTime))) times.Add(time, new List<double>());
    }
}
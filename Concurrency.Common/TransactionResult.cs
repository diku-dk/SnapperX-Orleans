using Utilities;

namespace Concurrency.Common;

[GenerateSerializer]
public class TransactionResult : FunctionResult
{
    [Id(0)]
    public Dictionary<BreakDownLatency, double> time;

    public TransactionResult() => time = new Dictionary<BreakDownLatency, double>();

    public void Print()
    {
        foreach (BreakDownLatency t in Enum.GetValues(typeof(BreakDownLatency)))
            Console.WriteLine($"{t} = {time[t]} ms");
    }
}
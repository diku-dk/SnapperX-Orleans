namespace Experiment.Common;

public interface IWorkloadGenerator
{
    public Dictionary<int, Queue<(bool, RequestData)>> GenerateSimpleWorkload();
}
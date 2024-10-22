using Utilities;

namespace Concurrency.Common;

[GenerateSerializer]
public class FunctionResult
{
    /// <summary>  the returned result of a specific function call on the customer grain </summary>
    [Id(0)]
    public object? resultObj;

    /// <summary> the first exception collected while executing a transaction </summary>
    [Id(1)]
    public ExceptionType? exception;

    /// <summary> the grain accessed by the transaction, if any locks are granted to the transaction </summary>
    [Id(2)]
    public HashSet<SnapperGrainID> grains;

    /// <summary> the time when the schedule info is available (mainly for batch info to be registered on the grian)  </summary>
    [Id(3)]
    public DateTime timeToGetScheduleReady;

    /// <summary> the time to get turn to execute on the first accessed grain </summary>
    [Id(4)]
    public DateTime timeToGetTurn;

    /// <summary> the time to get access to the state of the first grain </summary>
    [Id(5)]
    public DateTime timeToGetState;

    /// <summary> the time to finish execution </summary>
    [Id(6)]
    public DateTime timeToFinishExecution;

    public FunctionResult()
    {
        exception = null;
        resultObj = null;
        grains = new HashSet<SnapperGrainID>();
        timeToGetScheduleReady = DateTime.MinValue;
        timeToGetTurn = DateTime.MinValue;
        timeToGetState = DateTime.MinValue;
        timeToFinishExecution = DateTime.MinValue;
    }

    public void SetResult(object resultObj) => this.resultObj = resultObj;

    public void AddGrain(SnapperGrainID grainID) => grains.Add(grainID);
    
    public void RemoveGrain(SnapperGrainID grainID) => grains.Remove(grainID);
    
    public void SetException(ExceptionType e)
    {
        if (exception != null) return;
        
        exception = e;
    }

    public void MergeResult(FunctionResult res)
    {
        grains.UnionWith(res.grains);

        if (exception != null || res.exception == null) return;

        exception = res.exception;
    }

    public bool hasException() => exception != null;
}
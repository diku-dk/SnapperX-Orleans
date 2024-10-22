using Concurrency.Common;
using Concurrency.Common.ICache;
using Replication.Implementation.TransactionReplication;
using Replication.Interface;
using System.Diagnostics;
using TestApp.Grains.State;
using TestApp.Interfaces;
using Utilities;

namespace TestApp.Grains;

public class SnapperReplicatedTestGrain : TransactionReplicationGrain, ISnapperReplicatedTestGrain
{
    public SnapperReplicatedTestGrain(IHistoryManager historyManager, ISnapperReplicaCache snapperReplicaCache, ISnapperClusterCache snapperClusterCache)
        : base(typeof(SnapperReplicatedTestGrain).FullName, historyManager, snapperReplicaCache, snapperClusterCache) { }

    public async Task<object?> DoOp(TransactionContext cxt, object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        var myGrainID = new GrainID(Guid.Parse(strs[0]), typeof(SnapperReplicatedTestGrain).FullName);

        Debug.Assert(obj != null);
        var input = obj as TestFuncInput;
        Debug.Assert(input != null);
        Debug.Assert(input.info.ContainsKey(myGrainID));
        Debug.Assert(input.info.Count() == 1);

        // do operation on the current grain
        var info = input.info.First().Value;
        var opOnCurrentGrain = info.Item1;
        if (opOnCurrentGrain != AccessMode.NoOp) _ = await GetState(cxt);

        // call other grains if needed
        var task = new List<Task>();
        foreach (var grainsInfo in info.Item3.info)
        {
            Debug.Assert(!grainsInfo.Key.Equals(myGrainID));
            var method = typeof(SnapperReplicatedTestGrain).GetMethod("DoOp");
            Debug.Assert(method != null);
            var newInput = new Dictionary<GrainID, (AccessMode, OperationType, TestFuncInput)> { { grainsInfo.Key, (grainsInfo.Value.Item1, grainsInfo.Value.Item2, grainsInfo.Value.Item3) } };
            var funcCall = new FunctionCall(method, new TestFuncInput(newInput));
            task.Add(CallGrain(grainsInfo.Key, funcCall, cxt));
        }
        await Task.WhenAll(task);
        return null;
    }
}
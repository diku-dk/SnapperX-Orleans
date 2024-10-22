using Utilities;
using TestApp.Grains;
using Concurrency.Common;
using System.Diagnostics;
using Concurrency.Interface.GrainPlacement;
using Replication.Interface.GrainReplicaPlacement;
using Concurrency.Common.Cache;
using Experiment.Common;

namespace TestApp.Workload;

public class Benchmark : IBenchmark
{
    readonly string myRegionID;

    readonly bool isDet;
    readonly ImplementationType implementationType;
    readonly List<IGrainPlacementManager> pmList;
    readonly IClusterClient client;
    readonly Random rnd;

    readonly List<IReplicaGrainPlacementManager> replicaPmList;

    public Benchmark(string myRegionID, string mySiloID, string myReplicaSiloID, bool isDet, ImplementationType implementationType, IClusterClient client, StaticClusterInfo staticClusterInfo, StaticReplicaInfo? staticReplicaInfo)
    {
        this.myRegionID = myRegionID;
        this.isDet = isDet;
        this.implementationType = implementationType;
        pmList = staticClusterInfo.pmList[myRegionID][mySiloID].Select(pmID => client.GetGrain<IGrainPlacementManager>(pmID, myRegionID + "+" + mySiloID)).ToList();
        this.client = client;
        rnd = new Random();

        if (staticReplicaInfo != null)
            replicaPmList = staticReplicaInfo.pmList[myRegionID][myReplicaSiloID].Select(pmID => client.GetGrain<IReplicaGrainPlacementManager>(pmID, myRegionID + "+" + myReplicaSiloID)).ToList();
        else replicaPmList = new List<IReplicaGrainPlacementManager>();
    }

    public Task<TransactionResult> NewTransaction(RequestData data, bool isReplica)
    {
        switch (implementationType)
        {
            case ImplementationType.SNAPPER:
            case ImplementationType.SNAPPERSIMPLE:
                return NewSnapperTransaction(data, isReplica);
            case ImplementationType.ORLEANSTXN: return NewOrleansTransaction(data, isReplica);
            case ImplementationType.NONTXN: return NewEventualTransaction(data, isReplica);
            default:
                throw new Exception($"The implementationType {implementationType} is not supported");
        }
    }

    Task<TransactionResult> NewSnapperTransaction(RequestData data, bool isReplica)
    {
        var transactionType = Enum.Parse<TestAppTxnType>(data.transactionType);
        Debug.Assert(data.funcInput != null);

        if (!isReplica)
        {
            // randomly select a PM to submit the transaction
            var pm = pmList[rnd.Next(0, pmList.Count)];

            var method = typeof(SnapperTransactionalTestGrain).GetMethod(data.transactionType);
            Debug.Assert(method != null);
            var startFunc = new FunctionCall(method, data.funcInput);

            if (isDet) return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains);
            else return pm.SubmitTransaction(data.grains.First(), startFunc);
        }
        else
        {
            // randomly select a PM to submit the transaction
            var pm = replicaPmList[rnd.Next(0, replicaPmList.Count)];

            switch (transactionType)
            {
                case TestAppTxnType.DoOp:
                    var method = typeof(SnapperReplicatedTestGrain).GetMethod(data.transactionType);
                    Debug.Assert(method != null);
                    var startFunc = new FunctionCall(method, data.funcInput);
                    if (isDet) return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains);
                    else return pm.SubmitTransaction(data.grains.First(), startFunc);
                default:
                    throw new Exception($"The {transactionType} transaction is not supported for {BenchmarkType.TESTAPP} benchamrk in replica silo. ");
            }
        }
    }

    Task<TransactionResult> NewOrleansTransaction(RequestData data, bool isReplica)
    {
        //var txnGrain = client.GetGrain<IOrleansTransactionalAccountGrain>(data.grains.First().id, data.grains.First().location);
        //return txnGrain.StartTransaction(startFunc, funcInput);
        throw new NotImplementedException();
    }

    Task<TransactionResult> NewEventualTransaction(RequestData data, bool isReplica)
    {
        //var eventuallyConsistentGrain = client.GetGrain<INonTransactionalAccountGrain>(data.grains.First().id, data.grains.First().location);
        //return eventuallyConsistentGrain.StartTransaction(startFunc, funcInput);
        throw new NotImplementedException();
    }
}
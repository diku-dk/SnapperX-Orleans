using Utilities;
using Concurrency.Common;
using System.Diagnostics;
using Concurrency.Interface.GrainPlacement;
using Replication.Interface.GrainReplicaPlacement;
using Concurrency.Common.Cache;
using Experiment.Common;
using SmallBank.Interfaces;

namespace SmallBank.Workload;

public class Benchmark : IBenchmark
{
    readonly string myRegionID;

    readonly bool isDet;
    readonly ImplementationType implementationType;
    readonly List<IGrainPlacementManager> pmList;
    readonly IClusterClient client;
    readonly Random rnd;

    readonly List<IReplicaGrainPlacementManager> replicaPmList;

    readonly Dictionary<GrainID, int> grainIDToPartitionID;
    readonly Dictionary<int, (string, string)> partitionIDToMasterInfo;

    public Benchmark(string myRegionID, string mySiloID, string myReplicaSiloID, bool isDet, ImplementationType implementationType, IClusterClient client, IEnvConfigure envConfigureHelper, StaticClusterInfo staticClusterInfo, StaticReplicaInfo? staticReplicaInfo)
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

        (grainIDToPartitionID, partitionIDToMasterInfo, var grainIDToMigrationWorker) = envConfigureHelper.GetGrainPlacementInfo();
    }

    public Task<TransactionResult> NewTransaction(RequestData data, bool isReplica)
    {
        switch (implementationType)
        {
            case ImplementationType.SNAPPER:
            case ImplementationType.SNAPPERSIMPLE:
                return NewSnapperTransaction(data, isReplica, false);
            case ImplementationType.SNAPPERFINE:
                return NewSnapperTransaction(data, isReplica, true);
            case ImplementationType.ORLEANSTXN:
                return NewOrleansTransaction(data);
            case ImplementationType.NONTXN:
                return NewNonTransaction(data);
            case ImplementationType.NONTXNKV:
                return NewNonKeyValueTransaction(data, isReplica);
            default:
                throw new Exception($"The implementationType {implementationType} is not supported");
        }
    }

    Task<TransactionResult> NewSnapperTransaction(RequestData data, bool isReplica, bool isFine)
    {
        Debug.Assert(data.funcInput != null);
        var startGrainID = data.grains.First();
        var method = GrainNameHelper.GetMethod(implementationType, startGrainID.className, data.transactionType);
        var startFunc = new FunctionCall(method, data.funcInput);

        if (!isReplica)
        {
            // randomly select a PM to submit the transaction
            var pm = pmList[rnd.Next(0, pmList.Count)];

            if (isDet)
            {
                if (isFine) return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains, data.accessedKeysPerGrain);
                return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains);

            }
            else return pm.SubmitTransaction(data.grains.First(), startFunc);
        }
        else
        {
            // randomly select a PM to submit the transaction
            var pm = replicaPmList[rnd.Next(0, replicaPmList.Count)];

            if (isDet) return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains);
            else return pm.SubmitTransaction(data.grains.First(), startFunc);
        }
    }

    Task<TransactionResult> NewOrleansTransaction(RequestData data)
    {
        Debug.Assert(data.funcInput != null);
        if (Constants.noDeadlock) data.grains.Sort();
        var startGrainID = data.grains.First();

        var grain = GetGrainReference<IOrleansTransactionalAccountGrain>(startGrainID);
        return grain.MultiTransfer(data.funcInput);
    }

    Task<TransactionResult> NewNonTransaction(RequestData data)
    {
        Debug.Assert(data.funcInput != null);
        var startGrainID = data.grains.First();

        var grain = GetGrainReference<INonTransactionalSimpleAccountGrain>(startGrainID);
        return grain.MultiTransfer(data.funcInput);
    }

    Task<TransactionResult> NewNonKeyValueTransaction(RequestData data, bool isReplica)
    {
        Debug.Assert(data.funcInput != null);
        var startGrainID = data.grains.First();
        var method = GrainNameHelper.GetMethod(implementationType, startGrainID.className, data.transactionType);
        var startFunc = new FunctionCall(method, data.funcInput);

        if (!isReplica)
        {
            // randomly select a PM to submit the transaction
            var pm = pmList[rnd.Next(0, pmList.Count)];

            return pm.SubmitNonTransactionalRequest(data.grains.First(), startFunc);
        }
        else
        {
            // randomly select a PM to submit the transaction
            var pm = replicaPmList[rnd.Next(0, replicaPmList.Count)];

            throw new NotImplementedException();
        }
    }

    T GetGrainReference<T>(GrainID grainID) where T : IGrainWithGuidCompoundKey
    {
        var partitionID = grainIDToPartitionID[grainID];
        (var regionID, var siloID) = partitionIDToMasterInfo[partitionID];
        var snapperGrainID = new SnapperGrainID(grainID.id, regionID + "+" + siloID, grainID.className);

        var grain = client.GetGrain<T>(snapperGrainID.grainID.id, snapperGrainID.location);
        return grain;
    }
}
using Concurrency.Common;
using Concurrency.Common.State;
using Experiment.Common;
using MathNet.Numerics.Distributions;
using System.Diagnostics;
using TestApp.Grains.State;
using Utilities;

namespace TestApp.Workload;

public class WorkloadGenerator : IWorkloadGenerator
{
    readonly int regionIndex;
    readonly int siloIndex;
    readonly int numEpoch;
    readonly WorkloadConfigure workload;
    readonly EnvSetting envSetting;
    readonly BasicEnvSetting basicEnvSetting;
    readonly IDiscreteDistribution accessModeDistribution = new DiscreteUniform(0, 99, new Random());           // [0, 99]
    readonly IDiscreteDistribution writeTypeDistribution = new DiscreteUniform(0, 99, new Random());            // [0, 99]
    readonly IDiscreteDistribution pactPercentDistribution = new DiscreteUniform(0, 99, new Random());          // [0, 99]
    readonly IDiscreteDistribution multiSiloPercentDistribution = new DiscreteUniform(0, 99, new Random());     // [0, 99]
    readonly IDiscreteDistribution multiRegionPercentDistribution = new DiscreteUniform(0, 99, new Random());   // [0, 99]
    readonly IDiscreteDistribution hotGrainPercentDistribution = new DiscreteUniform(0, 99, new Random());      // [0, 99]
    readonly IDiscreteDistribution txnTypeDistribution = new DiscreteUniform(0, 99, new Random());
    readonly List<(TestAppTxnType, int)> txnTypes;    // the percentage of different types of transactions

    // ================================================================================================= for replication
    readonly bool isReplica;
    readonly int numReplicaSiloPerRegion;
    readonly List<string> localReplicaSiloList;
    readonly Dictionary<string, List<int>> replicaPartitionIDsPerSilo;    // silo ID => list of replica partitions

    public WorkloadGenerator(
        bool isReplica,
        string myRegion,
        int regionIndex,
        int siloIndex,
        IEnvSetting envSetting,
        IEnvConfigure envConfigureHelper,
        WorkloadConfigure workload,

        List<string> localReplicaSiloList)
    {
        this.regionIndex = regionIndex;
        this.siloIndex = siloIndex;
        this.envSetting = (EnvSetting)envSetting;
        this.basicEnvSetting = envSetting.GetBasic();
        this.workload = workload;
        txnTypes = workload.txnTypes.Select(x => (x.Key, x.Value)).ToList();
        numEpoch = basicEnvSetting.GetNumEpochAndNumReRun().Item1;

        // ================================================================================================= for replication
        this.isReplica = isReplica;
        if (isReplica)
        {
            this.numReplicaSiloPerRegion = basicEnvSetting.numReplicaSiloPerRegion;
            Debug.Assert(localReplicaSiloList.Count == numReplicaSiloPerRegion);
            this.localReplicaSiloList = localReplicaSiloList;    // the list of local replica silos within the current region
            replicaPartitionIDsPerSilo = new Dictionary<string, List<int>>();
            var replicaPartitionIDToSiloID = envConfigureHelper.GetReplicaGrainPlacementInfo().Item2[myRegion];
            foreach (var item in replicaPartitionIDToSiloID)
            {
                if (!replicaPartitionIDsPerSilo.ContainsKey(item.Value)) replicaPartitionIDsPerSilo.Add(item.Value, new List<int>());
                replicaPartitionIDsPerSilo[item.Value].Add(item.Key);
            }

            // check that every replica silo has replica partitions
            localReplicaSiloList.ForEach(siloID => Debug.Assert(replicaPartitionIDsPerSilo.ContainsKey(siloID)));
        }

        Console.WriteLine($"TestAPP WorkloadGenerator: isReplica = {isReplica}, region index = {regionIndex}, silo index = {siloIndex}");
    }

    TestAppTxnType GetTxnType()
    {
        var sample = txnTypeDistribution.Sample();

        var value = 0;
        for (var i = 0; i < txnTypes.Count; i++)
        {
            value += txnTypes[i].Item2;
            Debug.Assert(value <= 100);
            if (sample < value) return txnTypes[i].Item1;
        }

        throw new Exception($"This should never happen. ");
    }

    AccessMode howToAccess()
    {
        if (isReplica) return AccessMode.Read;

        var sample = accessModeDistribution.Sample();
        if (sample < workload.noOpPercent) return AccessMode.NoOp;
        if (sample < workload.noOpPercent + workload.readOnlyPercent) return AccessMode.Read;
        return AccessMode.ReadWrite;
    }

    OperationType howToWrite()
    {
        var sample = writeTypeDistribution.Sample();
        if (sample < workload.updatePercent) return OperationType.Update;
        else return OperationType.Delete;
    }

    bool isPACT()
    {
        var sample = pactPercentDistribution.Sample();
        if (sample < workload.pactPercent) return true;
        return false;
    }

    bool isMultiSilo()
    {
        if (workload.numSiloPerTxn == 1) return false;

        var sample = multiSiloPercentDistribution.Sample();
        if (sample < workload.multiSiloPercent) return true;
        return false;
    }

    bool isMultiRegion()
    {
        if (basicEnvSetting.numRegion == 1) return false;
        if (workload.numRegionPerTxn == 1) return false;
        if (workload.numSiloPerTxn == 1) return false;

        var sample = multiRegionPercentDistribution.Sample();
        if (sample < workload.multiRegionPercent) return true;
        return false;
    }

    bool isHotGrain()
    {
        var sample = hotGrainPercentDistribution.Sample();
        if (sample < workload.hotGrainPercent) return true;
        return false;
    }

    /// <summary> generate the list of grains accessed by each transaction </summary>
    public Dictionary<int, Queue<(bool, RequestData)>> GenerateSimpleWorkload()
    {
        if (isReplica)
        {
            Debug.Assert(workload.numSiloPerTxn <= numReplicaSiloPerRegion);
            Debug.Assert(workload.numRegionPerTxn == 1);
        }
        else
        {
            Debug.Assert(workload.numSiloPerTxn <= basicEnvSetting.numSiloPerRegion);
            Debug.Assert(workload.numRegionPerTxn <= basicEnvSetting.numRegion);
        }

        var numTxnPerEpoch = Constants.BASE_NUM_TRANSACTION;
        if (basicEnvSetting.implementationType == ImplementationType.NONTXN) numTxnPerEpoch *= 2;

        var numHotGrainPerPartition = (int)(envSetting.numGrainPerPartition * workload.hotPercentPerPartition / 100.0);
        var hotGrainDistribution = new DiscreteUniform(0, numHotGrainPerPartition - 1, new Random());
        IDiscreteDistribution normalGrainDistribution;
        if (numHotGrainPerPartition == envSetting.numGrainPerPartition) normalGrainDistribution = hotGrainDistribution;
        else normalGrainDistribution = new DiscreteUniform(numHotGrainPerPartition, envSetting.numGrainPerPartition - 1, new Random());

        var shared_requests = new Dictionary<int, Queue<(bool, RequestData)>>();
        for (int epoch = 0; epoch < numEpoch; epoch++)
        {
            shared_requests.Add(epoch, new Queue<(bool, RequestData)>());
            for (int txn = 0; txn < numTxnPerEpoch; txn++)
            {
                var numRegionPerTxn = isMultiRegion() ? workload.numRegionPerTxn : 1;
                var numSiloPerTxn = isMultiSilo() ? workload.numSiloPerTxn : 1;
                var numGrainPerPartition = workload.numGrainPerTxn / (numSiloPerTxn * envSetting.numMasterPartitionPerLocalSilo);

                var grainsPerTxn = new List<GrainID>();
                for (var r = 0; r < numRegionPerTxn; r++)
                {
                    var rIndex = (regionIndex + r) % basicEnvSetting.numRegion;
                    for (var s = 0; s < numSiloPerTxn / numRegionPerTxn; s++)
                    {
                        if (isReplica)
                        {
                            Debug.Assert(r == 0);
                            Debug.Assert(s <= numReplicaSiloPerRegion);
                            var sIndex = (siloIndex + s) % numReplicaSiloPerRegion;

                            var replicaPartitionIDList = replicaPartitionIDsPerSilo[localReplicaSiloList[sIndex]];
                            for (var pIndex = 0; pIndex < replicaPartitionIDList.Count; pIndex++)
                            {
                                var partitionID = replicaPartitionIDList[pIndex];

                                // get some IDs within a partition
                                var idList = new HashSet<int>();
                                for (var g = 0; g < numGrainPerPartition; g++)
                                {
                                    IDiscreteDistribution distribution;
                                    if (isHotGrain()) distribution = hotGrainDistribution;
                                    else distribution = normalGrainDistribution;

                                    var grainID = distribution.Sample();
                                    while (idList.Contains(grainID)) grainID = distribution.Sample();
                                    idList.Add(grainID);
                                }

                                // convert the ID into final ID
                                var finalList = idList.Select(x =>
                                {
                                    var newID = partitionID * envSetting.numGrainPerPartition + x;
                                    return new GrainID(Helper.ConvertIntToGuid(newID), new GrainNameHelper().GetGrainClassName(basicEnvSetting.implementationType, isReplica));
                                });
                                grainsPerTxn.AddRange(finalList);
                            }
                        }
                        else
                        {
                            Debug.Assert(s <= basicEnvSetting.numSiloPerRegion);
                            var sIndex = (siloIndex + s) % basicEnvSetting.numSiloPerRegion;

                            for (var pIndex = 0; pIndex < envSetting.numMasterPartitionPerLocalSilo; pIndex++)
                            {
                                // get some IDs within a partition
                                var idList = new HashSet<int>();
                                for (var g = 0; g < numGrainPerPartition; g++)
                                {
                                    IDiscreteDistribution distribution;
                                    if (isHotGrain()) distribution = hotGrainDistribution;
                                    else distribution = normalGrainDistribution;

                                    var grainID = distribution.Sample();
                                    while (idList.Contains(grainID)) grainID = distribution.Sample();
                                    idList.Add(grainID);
                                }

                                // convert the ID into final ID
                                var finalList = idList.Select(x =>
                                {
                                    var newID = rIndex * basicEnvSetting.numSiloPerRegion * envSetting.numMasterPartitionPerLocalSilo * envSetting.numGrainPerPartition +
                                                                                   sIndex * envSetting.numMasterPartitionPerLocalSilo * envSetting.numGrainPerPartition +
                                                                                                                               pIndex * envSetting.numGrainPerPartition +
                                                                                                                                        x;
                                    return new GrainID(Helper.ConvertIntToGuid(newID), new GrainNameHelper().GetGrainClassName(basicEnvSetting.implementationType, isReplica));
                                });
                                grainsPerTxn.AddRange(finalList);
                            }
                        }
                    }
                }
                Debug.Assert(grainsPerTxn.Count == workload.numGrainPerTxn);
                Debug.Assert(grainsPerTxn.ToHashSet().Count == workload.numGrainPerTxn);

                // need to generate txn profile as well
                var txnInput = new TestFuncInput();
                var chainInput = new TestFuncInput();
                txnInput.info.Add(grainsPerTxn[0], (howToAccess(), howToWrite(), chainInput));
                var i = 1;
                while (i < workload.numGrainPerTxn)
                {
                    var input = new TestFuncInput();
                    for (var j = 0; j < workload.txnDepth && i < workload.numGrainPerTxn; j++)
                    {
                        if (j == 0) chainInput.info.Add(grainsPerTxn[i], (howToAccess(), howToWrite(), input));
                        else
                        {
                            var newInput = new TestFuncInput();
                            input.info.Add(grainsPerTxn[i], (howToAccess(), howToWrite(), newInput));
                            input = newInput;
                        }
                        i++;
                    }
                }

                RequestData request;
                var txnType = GetTxnType();
                var grain1 = grainsPerTxn.First();
                var otherGrains = grainsPerTxn.GetRange(1, workload.numGrainPerTxn - 1);
                ISnapperKey key1 = new Random().Next(0, 100) < 50 ?
                    new TestKeyV1(Helper.ConvertGuidToInt(grain1.id), "key") :
                    new TestKeyV2(Helper.ConvertGuidToInt(grain1.id), "info", "note");
                var grain2 = otherGrains.First();
                ISnapperKey key2 = new Random().Next(0, 100) < 50 ?
                    new TestKeyV1(Helper.ConvertGuidToInt(grain2.id), "key") :
                    new TestKeyV2(Helper.ConvertGuidToInt(grain2.id), "info", "note");
                Debug.Assert(!grain1.Equals(grain2));

                switch (txnType)
                {
                    case TestAppTxnType.DoOp:
                        request = new RequestData(numSiloPerTxn > 1, numRegionPerTxn > 1, txnType.ToString(), grainsPerTxn, null, txnInput);
                        break;
                    case TestAppTxnType.RegisterUpdateReference:
                    case TestAppTxnType.DeRegisterUpdateReference:
                        request = new RequestData(numSiloPerTxn > 1, numRegionPerTxn > 1, txnType.ToString(), new List<GrainID> { grain2, grain1 }, null, new ReferenceInfo(SnapperKeyReferenceType.ReplicateReference, grain1, key1, grain2, key1, new DefaultUpdateFunction()));
                        break;
                    case TestAppTxnType.RegisterDeleteReference:
                    case TestAppTxnType.DeRegisterDeleteReference:
                        request = new RequestData(numSiloPerTxn > 1, numRegionPerTxn > 1, txnType.ToString(), new List<GrainID> { grain2, grain1 }, null, new ReferenceInfo(SnapperKeyReferenceType.DeleteReference, grain1, key1, grain2, key2, new DefaultFunction()));
                        break;
                    default:
                        throw new Exception($"GenerateSimpleWorkload: the txn {txnType} is not supported for {BenchmarkType.TESTAPP} benchmark. ");
                }

                shared_requests[epoch].Enqueue((isPACT(), request));
            }
        }

        return shared_requests;
    }
}
using Concurrency.Common;
using Experiment.Common;
using MathNet.Numerics.Distributions;
using SmallBank.Grains;
using SmallBank.Grains.State;
using System.Diagnostics;
using Utilities;

namespace SmallBank.Workload;

public class WorkloadGenerator : IWorkloadGenerator
{
    readonly int regionIndex;
    readonly int siloIndex;
    readonly int numEpoch;
    readonly WorkloadConfigure workload;
    readonly EnvSetting envSetting;
    readonly BasicEnvSetting basicEnvSetting;
    readonly IDiscreteDistribution keyDistribution;
    readonly IDiscreteDistribution moneyDistribution = new DiscreteUniform(1, 100, new Random());               // [1, 100]
    readonly IDiscreteDistribution readRateDistribution = new DiscreteUniform(0, 99, new Random());             // [0, 99]
    readonly IDiscreteDistribution pactPercentDistribution = new DiscreteUniform(0, 99, new Random());          // [0, 99]
    readonly IDiscreteDistribution multiSiloPercentDistribution = new DiscreteUniform(0, 99, new Random());     // [0, 99]
    readonly IDiscreteDistribution multiRegionPercentDistribution = new DiscreteUniform(0, 99, new Random());   // [0, 99]
    readonly IDiscreteDistribution hotKeyPercentDistribution = new DiscreteUniform(0, 99, new Random());        // [0, 99]
    readonly IDiscreteDistribution hotGrainPercentDistribution = new DiscreteUniform(0, 99, new Random());      // [0, 99]
    readonly IDiscreteDistribution txnTypeDistribution = new DiscreteUniform(0, 99, new Random());
    readonly List<(SmallBankTxnType, int)> txnTypes;    // the percentage of different types of transactions

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
        keyDistribution = new DiscreteUniform(0, this.envSetting.numAccountPerGrain - 1, new Random());

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

        Console.WriteLine($"SmallBank WorkloadGenerator: isReplica = {isReplica}, region index = {regionIndex}, silo index = {siloIndex}");
    }

    SmallBankTxnType GetTxnType()
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

    AccessMode GetAccessMode()
    {
        var sample = readRateDistribution.Sample();
        if (sample < workload.readKeyPercent) return AccessMode.Read;
        return AccessMode.ReadWrite;
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

    bool isHotKey()
    {
        var sample = hotKeyPercentDistribution.Sample();
        if (sample < workload.hotKeyPercent) return true;
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
            Debug.Assert((envSetting.numGrainPerPartition * workload.hotPercentPerPartition) % 100 == 0);

            Debug.Assert(workload.numKeyToAccessPerGrain <= envSetting.numAccountPerGrain);
            Debug.Assert((envSetting.numAccountPerGrain * workload.hotPercentPerGrain) % 100 == 0);
        }

        var numTxnPerEpoch = Constants.BASE_NUM_TRANSACTION;
        if (basicEnvSetting.implementationType == ImplementationType.NONTXN) numTxnPerEpoch *= 2;
        if (basicEnvSetting.isGrainMigrationExp) numTxnPerEpoch *= 10;

        var numHotGrainPerPartition = (int)(envSetting.numGrainPerPartition * workload.hotPercentPerPartition / 100.0);
        var hotGrainDistribution = new DiscreteUniform(0, numHotGrainPerPartition - 1, new Random());
        IDiscreteDistribution normalGrainDistribution;
        if (numHotGrainPerPartition == envSetting.numGrainPerPartition) normalGrainDistribution = hotGrainDistribution;
        else normalGrainDistribution = new DiscreteUniform(numHotGrainPerPartition, envSetting.numGrainPerPartition - 1, new Random());

        var grainName = new GrainNameHelper().GetGrainClassName(basicEnvSetting.implementationType, isReplica);

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
                                    return new GrainID(Helper.ConvertIntToGuid(newID), grainName);
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
                                    return new GrainID(Helper.ConvertIntToGuid(newID), grainName);
                                });
                                grainsPerTxn.AddRange(finalList);
                            }
                        }
                    }
                }

                Debug.Assert(grainsPerTxn.Count == workload.numGrainPerTxn);
                Debug.Assert(grainsPerTxn.ToHashSet().Count == workload.numGrainPerTxn);

                // ==============================================================================
                // calculate the multi-transfer input data
                var numHotKeyPerGrain = (int)(envSetting.numAccountPerGrain * workload.hotPercentPerGrain / 100.0);
                var hotKeyDistribution = new DiscreteUniform(0, numHotKeyPerGrain - 1, new Random());
                IDiscreteDistribution normalKeyDistribution;
                if (numHotKeyPerGrain == envSetting.numAccountPerGrain) normalKeyDistribution = hotKeyDistribution;
                else normalKeyDistribution = new DiscreteUniform(numHotKeyPerGrain, envSetting.numAccountPerGrain - 1, new Random());

                // generate the set of keys to access
                var keyIDsPerGrain = new Dictionary<GrainID, List<int>>();
                foreach (var grainID in grainsPerTxn)
                {
                    var keyIDs = new HashSet<int>();

                    if (workload.numKeyToAccessPerGrain == envSetting.numAccountPerGrain)
                    {
                        for (var i = 0; i < workload.numKeyToAccessPerGrain; i++) keyIDs.Add(i);
                    }
                    else
                    {
                        for (var i = 0; i < workload.numKeyToAccessPerGrain; i++)
                        {
                            if (isHotKey())
                            {
                                var keyID = hotKeyDistribution.Sample();
                                while (keyIDs.Contains(keyID)) keyID = hotKeyDistribution.Sample();
                                keyIDs.Add(keyID);
                            }
                            else
                            {
                                var keyID = normalKeyDistribution.Sample();
                                while (keyIDs.Contains(keyID)) keyID = normalKeyDistribution.Sample();
                                keyIDs.Add(keyID);
                            }
                        }
                        Debug.Assert(keyIDs.Count == workload.numKeyToAccessPerGrain);
                    }
                    
                    keyIDsPerGrain.Add(grainID, keyIDs.ToList());
                }
                
                // generate key access info for each grain
                var fromGrain = grainsPerTxn.First();
                var toGrains = grainsPerTxn.GetRange(1, workload.numGrainPerTxn - 1);
                var writeInfo = new Dictionary<UserID, Dictionary<UserID, int>>();
                var readInfo = new HashSet<UserID>();
                for (var i = 0; i < keyIDsPerGrain[fromGrain].Count; i++)
                {
                    var id = keyIDsPerGrain[fromGrain][i];
                    var fromUser = SmallBankIdMapping.GetUserID(fromGrain, id);

                    if (GetAccessMode() == AccessMode.ReadWrite)
                    {
                        writeInfo.Add(fromUser, new Dictionary<UserID, int>());

                        foreach (var toGrain in toGrains)
                        {
                            var toUserID = keyIDsPerGrain[toGrain][i];
                            var toUser = SmallBankIdMapping.GetUserID(toGrain, toUserID);
                            writeInfo[fromUser].Add(toUser, moneyDistribution.Sample());
                        }
                    }
                    else
                    {
                        readInfo.Add(fromUser);

                        foreach (var toGrain in toGrains)
                        {
                            var toUserID = keyIDsPerGrain[toGrain][i];
                            var toUser = SmallBankIdMapping.GetUserID(toGrain, toUserID);
                            readInfo.Add(toUser);
                        }
                    }
                }

                var multiTransferInput = new MultiTransferInput(writeInfo, readInfo);
                var keyAccessInfo = multiTransferInput.GetKeyAccessInfo(grainName);
                //multiTransferInput.Print(grainName);
                RequestData request;
                var txnType = GetTxnType();
                switch (txnType)
                {
                    case SmallBankTxnType.MultiTransfer:
                        request = new RequestData(numSiloPerTxn > 1, numRegionPerTxn > 1, txnType.ToString(), grainsPerTxn, keyAccessInfo, multiTransferInput);
                        break;
                    case SmallBankTxnType.CollectBalance:
                        request = new RequestData(numSiloPerTxn > 1, numRegionPerTxn > 1, txnType.ToString(), grainsPerTxn, null, null);
                        break;
                    default:
                        throw new Exception($"GenerateSimpleWorkload: the txn {txnType} is not supported for {BenchmarkType.SMALLBANK} benchmark. ");
                }

                shared_requests[epoch].Enqueue((isPACT(), request));
            }
        }
        return shared_requests;
    }
}
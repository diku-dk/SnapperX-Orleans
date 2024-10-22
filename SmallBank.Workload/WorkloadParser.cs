using Experiment.Common;
using System.Diagnostics;
using System.Xml;
using Utilities;

namespace SmallBank.Workload;

public class WorkloadParser
{
    public static List<IWorkloadConfigure> ParseWorkloadFromXMLFile(string experimentID, bool isGrainMigrationExp, IEnvSetting envSetting)
    {
        var workloadGroup = new List<IWorkloadConfigure>();

        var path = Constants.dataPath + @$"XML\{BenchmarkType.SMALLBANK}\Exp{experimentID}.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var numGrainPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numGrainPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numKeyToAccessPerGrainGroup = Array.ConvertAll(rootNode.SelectSingleNode("numKeyToAccessPerGrain").FirstChild.Value.Split(","), x => int.Parse(x));

        var MultiTransferGroup = Array.ConvertAll(rootNode.SelectSingleNode("MultiTransfer").FirstChild.Value.Split(","), x => int.Parse(x));
        
        var numPartitionPerSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numPartitionPerSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numRegionPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numRegionPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));

        var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiSiloPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiSiloPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiRegionPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiRegionPercent").FirstChild.Value.Split(","), x => int.Parse(x));

        var readKeyPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("readKeyPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotKeyPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotKeyPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotPercentPerGrainGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotPercentPerGrain").FirstChild.Value.Split(","), x => double.Parse(x));
        var hotGrainPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotGrainPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotPercentPerPartitionGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotPercentPerPartition").FirstChild.Value.Split(","), x => double.Parse(x));
        
        var actPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
        var pactPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));

        var numActConsumerGroup = Array.ConvertAll(rootNode.SelectSingleNode("numActConsumer").FirstChild.Value.Split(","), x => int.Parse(x));
        var numPactConsumerGroup = Array.ConvertAll(rootNode.SelectSingleNode("numPactConsumer").FirstChild.Value.Split(","), x => int.Parse(x));

        var migrationPipeSizeGroup = new int[actPipeSizeGroup.Length];
        var numMigrationConsumerGroup = new int[actPipeSizeGroup.Length];
        if (isGrainMigrationExp)
        {
            migrationPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("migrationPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            numMigrationConsumerGroup = Array.ConvertAll(rootNode.SelectSingleNode("numMigrationConsumer").FirstChild.Value.Split(","), x => int.Parse(x));
        }

        // re-set act pipe size
        var smallbankEnv = envSetting as EnvSetting;
        Debug.Assert(smallbankEnv != null);
        var basicEnvSetting = envSetting.GetBasic();
        var totalNumGrain = smallbankEnv.numGrainPerPartition * smallbankEnv.numMasterPartitionPerLocalSilo;
        var totalNumKey = smallbankEnv.numAccountPerGrain * totalNumGrain;

        for (var i = 0; i < numGrainPerTxnGroup.Length; i++)
        {
            var numGrainPerTxn = numGrainPerTxnGroup[i];
            var numKeyToAccessPerGrain = numKeyToAccessPerGrainGroup[i];

            for (var j = 0; j < numPartitionPerSiloPerTxnGroup.Length; j++)
            {
                var txnTypes = new Dictionary<SmallBankTxnType, int>
                {
                    { SmallBankTxnType.MultiTransfer, MultiTransferGroup[j] }
                };

                var numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxnGroup[j];
                var numSiloPerTxn = numSiloPerTxnGroup[j];
                var numRegionPerTxn = numRegionPerTxnGroup[j];

                var pactPercent = pactPercentGroup[j];
                var multiSiloPercent = multiSiloPercentGroup[j];
                var multiRegionPercent = multiRegionPercentGroup[j];

                for (var m = 0; m < readKeyPercentGroup.Length; m++)
                {
                    var readKeyPercent = readKeyPercentGroup[m];
                    var hotKeyPercent = hotKeyPercentGroup[m];
                    var hotPercentPerGrain = hotPercentPerGrainGroup[m];
                    var hotGrainPercent = hotGrainPercentGroup[m];
                    var hotPercentPerPartition = hotPercentPerPartitionGroup[m];

                    for (int k = 0; k < actPipeSizeGroup.Length; k++)
                    {
                        var actPipeSize = actPipeSizeGroup[k];
                        var pactPipeSize = pactPipeSizeGroup[k];
                        var numActConsumer = numActConsumerGroup[k];
                        var numPactConsumer = numPactConsumerGroup[k];

                        var migrationPipeSize = migrationPipeSizeGroup[k];
                        var numMigrationConsumer = numMigrationConsumerGroup[k];

                        actPipeSize = Helper.GetACTPipeSize(basicEnvSetting.implementationType, totalNumGrain, totalNumKey, actPipeSize);

                        Debug.Assert(numGrainPerTxn >= numPartitionPerSiloPerTxn * numSiloPerTxn);
                        Debug.Assert(numGrainPerTxn % numSiloPerTxn == 0);
                        Debug.Assert(numGrainPerTxn >= numRegionPerTxn);
                        Debug.Assert(numGrainPerTxn % numRegionPerTxn == 0);
                        Debug.Assert(numSiloPerTxn % numRegionPerTxn == 0);
                        Debug.Assert(txnTypes.Values.Sum() == 100);

                        if (!isGrainMigrationExp)
                        {
                            Debug.Assert(migrationPipeSize == 0);
                            Debug.Assert(numMigrationConsumer == 0);
                        }

                        var workload = new WorkloadConfigure(
                                            numGrainPerTxn,
                                            numKeyToAccessPerGrain,

                                            txnTypes,

                                            numPartitionPerSiloPerTxn,
                                            numSiloPerTxn,
                                            numRegionPerTxn,

                                            pactPercent,
                                            multiSiloPercent,
                                            multiRegionPercent,

                                            readKeyPercent,
                                            hotKeyPercent,
                                            hotPercentPerGrain,
                                            hotGrainPercent,
                                            hotPercentPerPartition,

                                            actPipeSize,
                                            pactPipeSize,
                                            numActConsumer,
                                            numPactConsumer,

                                            migrationPipeSize,
                                            numMigrationConsumer);

                        workloadGroup.Add(workload);
                    }
                }
            }
        }
        return workloadGroup;
    }

    public static List<IWorkloadConfigure> ParseReplicaWorkloadFromXMLFile(string experimentID, bool isGrainMigrationExp, ImplementationType implementationType)
    {
        var workloadGroup = new List<IWorkloadConfigure>();

        var path = Constants.dataPath + @$"XML\{BenchmarkType.SMALLBANK}\Exp{experimentID}-{implementationType}-REPLICA.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var numGrainPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numGrainPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numKeyToAccessPerGrainGroup = Array.ConvertAll(rootNode.SelectSingleNode("numKeyToAccessPerGrain").FirstChild.Value.Split(","), x => int.Parse(x));

        var CollectBalanceGroup = Array.ConvertAll(rootNode.SelectSingleNode("CollectBalance").FirstChild.Value.Split(","), x => int.Parse(x));

        var numPartitionPerSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numPartitionPerSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        
        var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiSiloPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiSiloPercent").FirstChild.Value.Split(","), x => int.Parse(x));

        var hotKeyPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotKeyPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotPercentPerGrainGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotPercentPerGrain").FirstChild.Value.Split(","), x => double.Parse(x));
        var hotGrainPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotGrainPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotPercentPerPartitionGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotPercentPerPartition").FirstChild.Value.Split(","), x => double.Parse(x));
        
        var actPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
        var pactPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
        var numActConsumerGroup = Array.ConvertAll(rootNode.SelectSingleNode("numActConsumer").FirstChild.Value.Split(","), x => int.Parse(x));
        var numPactConsumerGroup = Array.ConvertAll(rootNode.SelectSingleNode("numPactConsumer").FirstChild.Value.Split(","), x => int.Parse(x));

        var migrationPipeSizeGroup = new int[actPipeSizeGroup.Length];
        var numMigrationConsumerGroup = new int[actPipeSizeGroup.Length];
        if (isGrainMigrationExp)
        {
            migrationPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("migrationPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            numMigrationConsumerGroup = Array.ConvertAll(rootNode.SelectSingleNode("numMigrationConsumer").FirstChild.Value.Split(","), x => int.Parse(x));
        }

        for (var i = 0; i < numGrainPerTxnGroup.Length; i++)
        {
            var numGrainPerTxn = numGrainPerTxnGroup[i];
            var numKeyToAccessPerGrain = numKeyToAccessPerGrainGroup[i];

            var txnTypes = new Dictionary<SmallBankTxnType, int>
            {
                { SmallBankTxnType.CollectBalance, CollectBalanceGroup[i] }
            };

            var numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxnGroup[i];
            var numSiloPerTxn = numSiloPerTxnGroup[i];

            var pactPercent = pactPercentGroup[i];
            var multiSiloPercent = multiSiloPercentGroup[i];

            var hotKeyPercent = hotKeyPercentGroup[i];
            var hotPercentPerGrain = hotPercentPerGrainGroup[i];
            var hotGrainPercent = hotGrainPercentGroup[i];
            var hotPercentPerPartition = hotPercentPerPartitionGroup[i];

            for (int j = 0; j < actPipeSizeGroup.Length; j++)
            {
                var actPipeSize = actPipeSizeGroup[j];
                var pactPipeSize = pactPipeSizeGroup[j];
                var numActConsumer = numActConsumerGroup[j];
                var numPactConsumer = numPactConsumerGroup[j];

                var migrationPipeSize = migrationPipeSizeGroup[j];
                var numMigrationConsumer = numMigrationConsumerGroup[j];

                Debug.Assert(numGrainPerTxn >= numPartitionPerSiloPerTxn * numSiloPerTxn);
                Debug.Assert(numGrainPerTxn % numSiloPerTxn == 0);
                Debug.Assert(txnTypes.Values.Sum() == 100);

                if (!isGrainMigrationExp)
                {
                    Debug.Assert(migrationPipeSize == 0);
                    Debug.Assert(numMigrationConsumer == 0);
                }

                var workload = new WorkloadConfigure(
                    numGrainPerTxn,
                    numKeyToAccessPerGrain,

                    txnTypes,

                    numPartitionPerSiloPerTxn,
                    numSiloPerTxn,

                    pactPercent,
                    multiSiloPercent,

                    hotKeyPercent,
                    hotPercentPerGrain,
                    hotGrainPercent,
                    hotPercentPerPartition,

                    actPipeSize,
                    pactPipeSize,
                    numActConsumer,
                    numPactConsumer,

                    migrationPipeSize,
                    numMigrationConsumer);

                workloadGroup.Add(workload);
            }
        }
        return workloadGroup;
    }
}
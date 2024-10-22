using Experiment.Common;
using System.Diagnostics;
using System.Xml;
using Utilities;

namespace TestApp.Workload;

public class WorkloadParser : IWorkloadParser
{
    public static List<IWorkloadConfigure> ParseWorkloadFromXMLFile(string experimentID, bool isGrainMigrationExp, ImplementationType implementationType)
    {
        var workloadGroup = new List<IWorkloadConfigure>();

        var path = Constants.dataPath + @$"XML\{BenchmarkType.TESTAPP}\Exp{experimentID}-{implementationType}.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var numGrainPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numGrainPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));

        var txnDepthGroup = Array.ConvertAll(rootNode.SelectSingleNode("txnDepth").FirstChild.Value.Split(","), x => int.Parse(x));
        var noOpPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("noOpPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var readOnlyPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("readOnlyPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var updatePercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("updatePercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var deletePercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("deletePercent").FirstChild.Value.Split(","), x => int.Parse(x));

        var DoOpGroup = Array.ConvertAll(rootNode.SelectSingleNode("DoOp").FirstChild.Value.Split(","), x => int.Parse(x));
        var RegisterUpdateReferenceGroup = Array.ConvertAll(rootNode.SelectSingleNode("RegisterUpdateReference").FirstChild.Value.Split(","), x => int.Parse(x));
        var DeRegisterUpdateReferenceGroup = Array.ConvertAll(rootNode.SelectSingleNode("DeRegisterUpdateReference").FirstChild.Value.Split(","), x => int.Parse(x));
        var RegisterDeleteReferenceGroup = Array.ConvertAll(rootNode.SelectSingleNode("RegisterDeleteReference").FirstChild.Value.Split(","), x => int.Parse(x));
        var DeRegisterDeleteReferenceGroup = Array.ConvertAll(rootNode.SelectSingleNode("DeRegisterDeleteReference").FirstChild.Value.Split(","), x => int.Parse(x));

        var numPartitionPerSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numPartitionPerSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numRegionPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numRegionPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));

        var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiSiloPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiSiloPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiRegionPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiRegionPercent").FirstChild.Value.Split(","), x => int.Parse(x));

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

            var txnDepth = txnDepthGroup[i];
            var noOpPercent = noOpPercentGroup[i];
            var readOnlyPercent = readOnlyPercentGroup[i];
            var updatePercent = updatePercentGroup[i];
            var deletePercent = deletePercentGroup[i];

            var txnTypes = new Dictionary<TestAppTxnType, int>
            {
                { TestAppTxnType.DoOp, DoOpGroup[i] },
                { TestAppTxnType.RegisterUpdateReference, RegisterUpdateReferenceGroup[i] },
                { TestAppTxnType.DeRegisterUpdateReference, DeRegisterUpdateReferenceGroup[i] },
                { TestAppTxnType.RegisterDeleteReference, RegisterDeleteReferenceGroup[i] },
                { TestAppTxnType.DeRegisterDeleteReference, DeRegisterDeleteReferenceGroup[i]}
            };

            var numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxnGroup[i];
            var numSiloPerTxn = numSiloPerTxnGroup[i];
            var numRegionPerTxn = numRegionPerTxnGroup[i];

            var pactPercent = pactPercentGroup[i];
            var multiSiloPercent = multiSiloPercentGroup[i];
            var multiRegionPercent = multiRegionPercentGroup[i];

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

                Debug.Assert(txnDepth >= 1);
                Debug.Assert(numGrainPerTxn > txnDepth);
                Debug.Assert(noOpPercent + readOnlyPercent <= 100);
                Debug.Assert(updatePercent + deletePercent == 100);
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

                                    txnDepth,
                                    noOpPercent,
                                    readOnlyPercent,
                                    updatePercent,
                                    deletePercent,

                                    txnTypes,

                                    numPartitionPerSiloPerTxn,
                                    numSiloPerTxn,
                                    numRegionPerTxn,

                                    pactPercent,
                                    multiSiloPercent,
                                    multiRegionPercent,

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

    public static List<IWorkloadConfigure> ParseReplicaWorkloadFromXMLFile(string experimentID, bool isGrainMigrationExp,ImplementationType implementationType)
    {
        var workloadGroup = new List<IWorkloadConfigure>();

        var path = Constants.dataPath + @$"XML\{BenchmarkType.TESTAPP}\Exp{experimentID}-{implementationType}-REPLICA.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var numGrainPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numGrainPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));

        var txnDepthGroup = Array.ConvertAll(rootNode.SelectSingleNode("txnDepth").FirstChild.Value.Split(","), x => int.Parse(x));

        var DoOpGroup = Array.ConvertAll(rootNode.SelectSingleNode("DoOp").FirstChild.Value.Split(","), x => int.Parse(x));

        var numPartitionPerSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numPartitionPerSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));
        var numSiloPerTxnGroup = Array.ConvertAll(rootNode.SelectSingleNode("numSiloPerTxn").FirstChild.Value.Split(","), x => int.Parse(x));

        var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiSiloPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiSiloPercent").FirstChild.Value.Split(","), x => int.Parse(x));

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

            var txnDepth = txnDepthGroup[i];

            var txnTypes = new Dictionary<TestAppTxnType, int>
            {
                { TestAppTxnType.DoOp, DoOpGroup[i] }
            };

            var numPartitionPerSiloPerTxn = numPartitionPerSiloPerTxnGroup[i];
            var numSiloPerTxn = numSiloPerTxnGroup[i];

            var pactPercent = pactPercentGroup[i];
            var multiSiloPercent = multiSiloPercentGroup[i];

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

                Debug.Assert(txnDepth >= 1);
                Debug.Assert(numGrainPerTxn > txnDepth);
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

                    txnDepth,

                    txnTypes,

                    numPartitionPerSiloPerTxn,
                    numSiloPerTxn,

                    pactPercent,
                    multiSiloPercent,

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
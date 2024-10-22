using Experiment.Common;
using System.Diagnostics;
using System.Xml;
using Utilities;

namespace Experiment.Controller;

public static class EnvConfigurationParser
{
    public static IEnvSetting GetConfigurationFromXMLFile(string experimentID, int index1, int index2)
    {
        var path = Constants.dataPath + @$"XML\ExpEnvConfigurations.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var numNodes = rootNode.ChildNodes.Count;
        for (int i = 1; i < numNodes; i++)
        {
            var node = rootNode.ChildNodes.Item(i);
            if (node.Attributes.Item(0).Value != experimentID) continue;

            var isLocalTest = Array.ConvertAll(node.SelectSingleNode("isLocalTest").FirstChild.Value.Trim().Split(","), bool.Parse)[index1];
            var isGrainMigrationExp = Array.ConvertAll(node.SelectSingleNode("isGrainMigrationExp").FirstChild.Value.Trim().Split(","), bool.Parse)[index1];
            var numRegion = Array.ConvertAll(node.SelectSingleNode("numRegion").FirstChild.Value.Split(","), int.Parse)[index1];
            var numSiloPerRegion = Array.ConvertAll(node.SelectSingleNode("numSiloPerRegion").FirstChild.Value.Split(","), int.Parse)[index1];
            var numReplicaSiloPerRegion = Array.ConvertAll(node.SelectSingleNode("numReplicaSiloPerRegion").FirstChild.Value.Split(","), int.Parse)[index1];
            var benchmark = Array.ConvertAll(node.SelectSingleNode("benchmark").FirstChild.Value.Split(","), Enum.Parse<BenchmarkType>)[index1];
            var implementationType = Array.ConvertAll(node.SelectSingleNode("implementationType").FirstChild.Value.Split(","), Enum.Parse<ImplementationType>)[index1];
            var doLogging = Array.ConvertAll(node.SelectSingleNode("doLogging").FirstChild.Value.Split(","), bool.Parse)[index1];
            var inRegionReplication = Array.ConvertAll(node.SelectSingleNode("inRegionReplication").FirstChild.Value.Split(","), bool.Parse)[index1];
            var crossRegionReplication = Array.ConvertAll(node.SelectSingleNode("crossRegionReplication").FirstChild.Value.Split(","), bool.Parse)[index1];
            var replicaWorkload = Array.ConvertAll(node.SelectSingleNode("replicaWorkload").FirstChild.Value.Split(","), bool.Parse)[index1];
            var speculativeACT = Array.ConvertAll(node.SelectSingleNode("speculativeACT").FirstChild.Value.Split(","), bool.Parse)[index1];
            var speculativeBatch = Array.ConvertAll(node.SelectSingleNode("speculativeBatch").FirstChild.Value.Split(","), bool.Parse)[index1];
            var globalBatchSizeInMSecs = Array.ConvertAll(node.SelectSingleNode("globalBatchSizeInMSecs").FirstChild.Value.Split(","), double.Parse)[index1];
            var regionalBatchSizeInMSecs = Array.ConvertAll(node.SelectSingleNode("regionalBatchSizeInMSecs").FirstChild.Value.Split(","), double.Parse)[index1];
            var localBatchSizeInMSecs = Array.ConvertAll(node.SelectSingleNode("localBatchSizeInMSecs").FirstChild.Value.Split(","), double.Parse)[index1];

            switch (benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    var numMasterPartitionPerLocalSilo1 = Array.ConvertAll(node.SelectSingleNode("numMasterPartitionPerLocalSilo").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numGrainPerPartition1 = Array.ConvertAll(node.SelectSingleNode("numGrainPerPartition").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numAccountPerGrain1 = Array.ConvertAll(node.SelectSingleNode("numAccountPerGrain").FirstChild.Value.Split(","), int.Parse)[index2];
                    return new SmallBank.Workload.EnvSetting(
                        isLocalTest,
                        isGrainMigrationExp,
                        numRegion, numSiloPerRegion, numReplicaSiloPerRegion, benchmark, implementationType,
                        doLogging, inRegionReplication, crossRegionReplication, replicaWorkload,
                        speculativeACT, speculativeBatch,
                        globalBatchSizeInMSecs, regionalBatchSizeInMSecs, localBatchSizeInMSecs,
                        numMasterPartitionPerLocalSilo1, numGrainPerPartition1, numAccountPerGrain1);
                case BenchmarkType.MARKETPLACE:
                    var numCityPerSilo = Array.ConvertAll(node.SelectSingleNode("numCityPerSilo").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numCustomerPerCity = Array.ConvertAll(node.SelectSingleNode("numCustomerPerCity").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numSellerPerCity = Array.ConvertAll(node.SelectSingleNode("numSellerPerCity").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numProductPerSeller = Array.ConvertAll(node.SelectSingleNode("numProductPerSeller").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numRegionPerSeller = Array.ConvertAll(node.SelectSingleNode("numRegionPerSeller").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numSiloPerSeller = Array.ConvertAll(node.SelectSingleNode("numSiloPerSeller").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numStockCityPerSeller = Array.ConvertAll(node.SelectSingleNode("numStockCityPerSeller").FirstChild.Value.Split(","), int.Parse)[index2];
                    var numProductPerStockCity = Array.ConvertAll(node.SelectSingleNode("numProductPerStockCity").FirstChild.Value.Split(","), int.Parse)[index2];
                    var initialNumProductPerCart = Array.ConvertAll(node.SelectSingleNode("initialNumProductPerCart").FirstChild.Value.Split(","), int.Parse)[index2];

                    var maxPricePerProduct = Array.ConvertAll(node.SelectSingleNode("maxPricePerProduct").FirstChild.Value.Split(","), int.Parse)[index2];
                    var maxQuantityInStockPerProduct = Array.ConvertAll(node.SelectSingleNode("maxQuantityInStockPerProduct").FirstChild.Value.Split(","), int.Parse)[index2];

                    Debug.Assert(numRegionPerSeller <= numRegion);
                    Debug.Assert(numSiloPerSeller <= numRegion * numSiloPerRegion);
                    Debug.Assert(numStockCityPerSeller <= numRegion * numSiloPerRegion * numCityPerSilo);
                    Debug.Assert(numSiloPerSeller % numRegionPerSeller == 0);
                    Debug.Assert(numStockCityPerSeller % numRegionPerSeller == 0);
                    Debug.Assert(numStockCityPerSeller % numSiloPerSeller == 0);
                    Debug.Assert(numProductPerStockCity <= numProductPerSeller);
                    Debug.Assert(initialNumProductPerCart <= numProductPerSeller * numSellerPerCity);

                    return new MarketPlace.Workload.EnvSetting(
                        isLocalTest,
                        isGrainMigrationExp,
                        numRegion, numSiloPerRegion, numReplicaSiloPerRegion, benchmark, implementationType,
                        doLogging, inRegionReplication, crossRegionReplication, replicaWorkload,
                        speculativeACT, speculativeBatch,
                        globalBatchSizeInMSecs, regionalBatchSizeInMSecs, localBatchSizeInMSecs,
                        numCityPerSilo, numCustomerPerCity, numSellerPerCity, numProductPerSeller,
                        numRegionPerSeller, numSiloPerSeller, numStockCityPerSeller, numProductPerStockCity,
                        initialNumProductPerCart,
                        maxPricePerProduct, maxQuantityInStockPerProduct);
            }
            
        }

        throw new Exception($"Fail to configuration for experimentID = {experimentID}, index1 = {index1}, index2 = {index2}");
    }
}
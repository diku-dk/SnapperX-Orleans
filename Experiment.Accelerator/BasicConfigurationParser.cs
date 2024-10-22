using System.Diagnostics;
using System.Xml;
using Utilities;

namespace Experiment.Accelerator;

internal static class BasicConfigurationParser
{
    public static List<BasicConfiguration> GetBasicConfigurationFromXMLFile()
    {
        var path = Constants.localDataPath + @$"XML\ExpEnvConfigurations.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;
        var numNodes = rootNode.ChildNodes.Count;

        var firstNode = rootNode.ChildNodes.Item(0);
        var experimentIDs = Array.ConvertAll(firstNode.FirstChild.Value.Trim().Split(","), int.Parse);

        var experiments = new List<BasicConfiguration>();
        foreach (var experimentID in experimentIDs)
        {
            for (int i = 1; i < numNodes; i++)
            {
                var node = rootNode.ChildNodes.Item(i);
                if (node.Attributes.Item(0).Value != experimentID.ToString()) continue;

                var isLocalTests = Array.ConvertAll(node.SelectSingleNode("isLocalTest").FirstChild.Value.Trim().Split(","), bool.Parse);
                var numRegions = Array.ConvertAll(node.SelectSingleNode("numRegion").FirstChild.Value.Split(","), int.Parse);
                var numSiloPerRegions = Array.ConvertAll(node.SelectSingleNode("numSiloPerRegion").FirstChild.Value.Split(","), int.Parse);
                var numReplicaSiloPerRegions = Array.ConvertAll(node.SelectSingleNode("numReplicaSiloPerRegion").FirstChild.Value.Split(","), int.Parse);
                var benchmarks = Array.ConvertAll(node.SelectSingleNode("benchmark").FirstChild.Value.Trim().Split(","), Enum.Parse<BenchmarkType>);

                Debug.Assert(isLocalTests.Length == numRegions.Length && numRegions.Length == numSiloPerRegions.Length && numSiloPerRegions.Length == numReplicaSiloPerRegions.Length);

                for (var index1 = 0; index1 < isLocalTests.Length; index1++)
                {
                    Debug.Assert(numRegions[index1] == 1);

                    var benchmark = benchmarks[index1];
                    var numIndex2 = 0;
                    switch (benchmark)
                    {
                        case BenchmarkType.SMALLBANK:
                            var numGrainPerPartitions = Array.ConvertAll(node.SelectSingleNode("numGrainPerPartition").FirstChild.Value.Split(","), int.Parse);
                            numIndex2 = numGrainPerPartitions.Length;
                            break;
                        case BenchmarkType.MARKETPLACE:
                            var numCityPerSilos = Array.ConvertAll(node.SelectSingleNode("numCityPerSilo").FirstChild.Value.Split(","), int.Parse);
                            numIndex2 = numCityPerSilos.Length;
                            break;
                        default:
                            throw new Exception($"The {benchmark} benchmark is not supported. ");
                    }

                    for (var index2 = 0; index2 < numIndex2; index2 ++)
                        experiments.Add(new BasicConfiguration(experimentID, index1, index2, numRegions[index1], numSiloPerRegions[index1], numReplicaSiloPerRegions[index1]));
                } 
            }
        }

        return experiments;
    }
}
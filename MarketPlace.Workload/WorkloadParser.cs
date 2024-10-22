using Experiment.Common;
using System.Diagnostics;
using System.Xml;
using Utilities;

namespace MarketPlace.Workload;

public class WorkloadParser
{
    public static List<IWorkloadConfigure> ParseWorkloadFromXMLFile(string experimentID, bool isGrainMigrationExp, ImplementationType implementationType)
    {
        var workloadGroup = new List<IWorkloadConfigure>();

        var path = Constants.dataPath + @$"XML\{BenchmarkType.MARKETPLACE}\Exp{experimentID}.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var AddItemToCartGroup = Array.ConvertAll(rootNode.SelectSingleNode("AddItemToCart").FirstChild.Value.Split(","), x => int.Parse(x));
        var DeleteItemInCartGroup = Array.ConvertAll(rootNode.SelectSingleNode("DeleteItemInCart").FirstChild.Value.Split(","), x => int.Parse(x));
        var CheckoutGroup = Array.ConvertAll(rootNode.SelectSingleNode("Checkout").FirstChild.Value.Split(","), x => int.Parse(x));
        
        var UpdatePriceGroup = Array.ConvertAll(rootNode.SelectSingleNode("UpdatePrice").FirstChild.Value.Split(","), x => int.Parse(x));
        var DeleteProductGroup = Array.ConvertAll(rootNode.SelectSingleNode("DeleteProduct").FirstChild.Value.Split(","), x => int.Parse(x));
        var AddProductsGroup = Array.ConvertAll(rootNode.SelectSingleNode("AddProducts").FirstChild.Value.Split(","), x => int.Parse(x));
        var ReplenishStockGroup = Array.ConvertAll(rootNode.SelectSingleNode("ReplenishStock").FirstChild.Value.Split(","), x => int.Parse(x));
        var AddStockGroup = Array.ConvertAll(rootNode.SelectSingleNode("AddStock").FirstChild.Value.Split(","), x => int.Parse(x));
        
        var hotCustomerPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotCustomerPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotCustomerPerCityGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotCustomerPerCity").FirstChild.Value.Split(","), x => double.Parse(x));

        var hotSellerPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotSellerPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotSellerPerCityGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotSellerPerCity").FirstChild.Value.Split(","), x => double.Parse(x));

        var hotProductPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotProductPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var hotProductPerSellerGroup = Array.ConvertAll(rootNode.SelectSingleNode("hotProductPerSeller").FirstChild.Value.Split(","), x => double.Parse(x));

        var maxNumProductPerCheckoutGroup = Array.ConvertAll(rootNode.SelectSingleNode("maxNumProductPerCheckout").FirstChild.Value.Split(","), x => int.Parse(x));
        var maxQuantityToBuyPerProductGroup = Array.ConvertAll(rootNode.SelectSingleNode("maxQuantityToBuyPerProduct").FirstChild.Value.Split(","), x => int.Parse(x));

        var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));
        var multiSiloPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("multiSiloPercent").FirstChild.Value.Split(","), x => int.Parse(x));

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

        for (var i = 0; i < AddItemToCartGroup.Length; i++)
        {
            var AddItemToCart = AddItemToCartGroup[i];
            var DeleteItemInCart = DeleteItemInCartGroup[i];
            var Checkout = CheckoutGroup[i];
            var AddProducts = AddProductsGroup[i];
            var UpdatePrice = UpdatePriceGroup[i];
            var DeleteProduct = DeleteProductGroup[i];
            var ReplenishStock = ReplenishStockGroup[i];
            var AddStock = AddStockGroup[i];

            var txnTypes = new Dictionary<MarketPlaceTxnType, int>
            {
                { MarketPlaceTxnType.AddItemToCart, AddItemToCart },
                { MarketPlaceTxnType.DeleteItemInCart, DeleteItemInCart },
                { MarketPlaceTxnType.Checkout, Checkout },
                { MarketPlaceTxnType.AddProducts, AddProducts },
                { MarketPlaceTxnType.UpdatePrice, UpdatePrice },
                { MarketPlaceTxnType.DeleteProduct, DeleteProduct },
                { MarketPlaceTxnType.ReplenishStock, ReplenishStock },
                { MarketPlaceTxnType.AddStock, AddStock }
            };
            Debug.Assert(txnTypes.Values.Sum() == 100);

            for (int j = 0; j < hotCustomerPercentGroup.Length; j++)
            {
                var hotCustomerPercent = hotCustomerPercentGroup[j];
                var hotCustomerPerCity = hotCustomerPerCityGroup[j];

                var hotSellerPercent = hotSellerPercentGroup[j];
                var hotSellerPerCity = hotSellerPerCityGroup[j];

                var hotProductPercent = hotProductPercentGroup[j];
                var hotProductPerSeller = hotProductPerSellerGroup[j];

                var maxNumProductPerCheckout = maxNumProductPerCheckoutGroup[j];
                var maxQuantityToBuyPerProduct = maxQuantityToBuyPerProductGroup[j];

                for (var k = 0; k < pactPercentGroup.Length; k++)
                {
                    var pactPercent = pactPercentGroup[k];
                    var multiSiloPercent = multiSiloPercentGroup[k];

                    for (var m = 0; m < actPipeSizeGroup.Length; m++)
                    {
                        var actPipeSize = actPipeSizeGroup[m];
                        var pactPipeSize = pactPipeSizeGroup[m];
                        var numActConsumer = numActConsumerGroup[m];
                        var numPactConsumer = numPactConsumerGroup[m];

                        var migrationPipeSize = migrationPipeSizeGroup[m];
                        var numMigrationConsumer = numMigrationConsumerGroup[m];

                        if (!isGrainMigrationExp)
                        {
                            Debug.Assert(migrationPipeSize == 0);
                            Debug.Assert(numMigrationConsumer == 0);
                        }

                        workloadGroup.Add(new WorkloadConfigure(
                            txnTypes,

                            hotCustomerPercent,
                            hotCustomerPerCity,

                            hotSellerPercent,
                            hotSellerPerCity,

                            hotProductPercent,
                            hotProductPerSeller,

                            maxNumProductPerCheckout,
                            maxQuantityToBuyPerProduct,

                            pactPercent,
                            multiSiloPercent,

                            actPipeSize,
                            pactPipeSize,
                            numActConsumer,
                            numPactConsumer,

                            migrationPipeSize,
                            numMigrationConsumer));
                    }
                }
            }
        }
        return workloadGroup;
    }

    public static List<IWorkloadConfigure> ParseReplicaWorkloadFromXMLFile(string experimentID, bool isGrainMigrationExp, ImplementationType implementationType)
    {
        var workloadGroup = new List<IWorkloadConfigure>();

        var path = Constants.dataPath + @$"XML\{BenchmarkType.MARKETPLACE}\Exp{experimentID}-REPLICA.xml";
        var xmlDoc = new XmlDocument();
        xmlDoc.Load(path);
        var rootNode = xmlDoc.DocumentElement;

        var RefreshSellerDashboardGroup = Array.ConvertAll(rootNode.SelectSingleNode("RefreshSellerDashboard").FirstChild.Value.Split(","), x => int.Parse(x));

        var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));

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

        for (var i = 0; i < RefreshSellerDashboardGroup.Length; i++)
        {
            var RefreshSellerDashboard = RefreshSellerDashboardGroup[i];

            var txnTypes = new Dictionary<MarketPlaceTxnType, int>
            {
                { MarketPlaceTxnType.RefreshSellerDashboard, RefreshSellerDashboard }
            };
            Debug.Assert(txnTypes.Values.Sum() == 100);

            for (int j = 0; j < actPipeSizeGroup.Length; j++)
            {
                var pactPercent = pactPercentGroup[j];

                var actPipeSize = actPipeSizeGroup[j];
                var pactPipeSize = pactPipeSizeGroup[j];
                var numActConsumer = numActConsumerGroup[j];
                var numPactConsumer = numPactConsumerGroup[j];

                var migrationPipeSize = migrationPipeSizeGroup[j];
                var numMigrationConsumer = numMigrationConsumerGroup[j];

                if (!isGrainMigrationExp)
                {
                    Debug.Assert(migrationPipeSize == 0);
                    Debug.Assert(numMigrationConsumer == 0);
                }

                workloadGroup.Add(new WorkloadConfigure(
                    txnTypes,

                    pactPercent,

                    actPipeSize,
                    pactPipeSize,
                    numActConsumer,
                    numPactConsumer,

                    migrationPipeSize,
                    numMigrationConsumer));
            }
        }

        return workloadGroup;
    }
}
using Experiment.Common;
using MessagePack;
using Utilities;

namespace MarketPlace.Workload;

[MessagePackObject(keyAsPropertyName: true)]
public class EnvSetting : IEnvSetting
{
    public readonly BasicEnvSetting basic;

    public readonly int numCityPerSilo;

    public readonly int numCustomerPerCity;

    public readonly int numSellerPerCity;

    /// <summary> the total number of products provided by each seller </summary>
    public readonly int numProductPerSeller;

    /// <summary> the number of regions the seller's stock actors are distributed </summary>
    public readonly int numRegionPerSeller;

    /// <summary> the total number of local silos the seller's stock actors are distributed </summary>
    public readonly int numSiloPerSeller;

    /// <summary> the total number of cities the seller's stock actors are distributed </summary>
    public readonly int numStockCityPerSeller;

    /// <summary> the number of products that have stock in a selected city </summary>
    public readonly int numProductPerStockCity;

    public readonly int initialNumProductPerCart;

    public readonly int maxPricePerProduct;

    public readonly int maxQuantityInStockPerProduct;

    public EnvSetting(BasicEnvSetting basic, int numCityPerSilo, int numCustomerPerCity, int numSellerPerCity, int numProductPerSeller, int numRegionPerSeller, int numSiloPerSeller, int numStockCityPerSeller, int numProductPerStockCity, int initialNumProductPerCart, int maxPricePerProduct, int maxQuantityInStockPerProduct)
    {
        this.basic = basic;
        this.numCityPerSilo = numCityPerSilo;
        this.numCustomerPerCity = numCustomerPerCity;
        this.numSellerPerCity = numSellerPerCity;
        this.numProductPerSeller = numProductPerSeller;
        this.numRegionPerSeller = numRegionPerSeller;
        this.numSiloPerSeller = numSiloPerSeller;
        this.numStockCityPerSeller = numStockCityPerSeller;
        this.numProductPerStockCity = numProductPerStockCity;
        this.initialNumProductPerCart = initialNumProductPerCart;
        this.maxPricePerProduct = maxPricePerProduct;
        this.maxQuantityInStockPerProduct = maxQuantityInStockPerProduct;
    }

    public EnvSetting(
        bool isLocalTest, bool isGrainMigrationExp, int numRegion, int numSiloPerRegion, int numReplicaSiloPerRegion,
        BenchmarkType benchmark, ImplementationType implementationType, bool doLogging,
        bool inRegionReplication, bool crossRegionReplication, bool replicaWorkload,
        bool speculativeACT, bool speculativeBatch,
        double globalBatchSizeInMSecs, double regionalBatchSizeInMSecs, double localBatchSizeInMSecs,
        int numCityPerSilo, int numCustomerPerCity, int numSellerPerCity, int numProductPerSeller,
        int numRegionPerSeller, int numSiloPerSeller, int numStockCityPerSeller, int numProductPerStockCity,
        int initialNumProductPerCart,
        int maxPricePerProduct, int maxQuantityInStockPerProduct)
    {
        basic = new BasicEnvSetting(isLocalTest, isGrainMigrationExp, numRegion, numSiloPerRegion, numReplicaSiloPerRegion,
            benchmark, implementationType, doLogging,
            inRegionReplication, crossRegionReplication, replicaWorkload,
            speculativeACT, speculativeBatch,
            globalBatchSizeInMSecs, regionalBatchSizeInMSecs, localBatchSizeInMSecs);
        this.numCityPerSilo = numCityPerSilo;
        this.numCustomerPerCity = numCustomerPerCity;
        this.numSellerPerCity = numSellerPerCity;
        this.numProductPerSeller = numProductPerSeller;

        this.numRegionPerSeller = numRegionPerSeller;
        this.numSiloPerSeller = numSiloPerSeller;
        this.numStockCityPerSeller = numStockCityPerSeller;
        this.numProductPerStockCity = numProductPerStockCity;

        this.initialNumProductPerCart = initialNumProductPerCart;

        this.maxPricePerProduct = maxPricePerProduct;
        this.maxQuantityInStockPerProduct = maxQuantityInStockPerProduct;
    }

    public int GetNumPartitionPerSilo() => numCityPerSilo;

    public BasicEnvSetting GetBasic() => basic;

    public void SetCredentialInfo(string redis_ConnectionString, string kafka_ConnectionString, string accessKey, string secretKey)
        => basic.SetCredentialInfo(redis_ConnectionString, kafka_ConnectionString, accessKey, secretKey);

    public void SetClusterInfo(Dictionary<SnapperRoleType, int> roleSizes, Dictionary<string, (int, int)> roleInfo)
        => basic.SetClusterInfo(roleSizes, roleInfo);

    public void PrintContent()
    {
        Console.WriteLine();
        Console.WriteLine($"=========================================================================================");
        basic.PrintContent();
        Console.WriteLine($"     EnvSetting: numCityPerSilo               = {numCityPerSilo}");
        Console.WriteLine($"     EnvSetting: numCustomerPerCity           = {numCustomerPerCity}");
        Console.WriteLine($"     EnvSetting: numSellerPerCity             = {numSellerPerCity}");
        Console.WriteLine($"     EnvSetting: numProductPerSeller          = {numProductPerSeller}");
        Console.WriteLine($"     EnvSetting: numRegionPerSeller           = {numRegionPerSeller}");
        Console.WriteLine($"     EnvSetting: numSiloPerSeller             = {numSiloPerSeller}");
        Console.WriteLine($"     EnvSetting: numStockCityPerSeller        = {numStockCityPerSeller}");
        Console.WriteLine($"     EnvSetting: numProductPerStockCity       = {numProductPerStockCity}");
        Console.WriteLine($"     EnvSetting: initialNumProductPerCart     = {initialNumProductPerCart}");
        Console.WriteLine($"     EnvSetting: maxPricePerProduct           = {maxPricePerProduct}");
        Console.WriteLine($"     EnvSetting: maxQuantityInStockPerProduct = {maxQuantityInStockPerProduct}");
        Console.WriteLine($"=========================================================================================");
        Console.WriteLine();
    }

    public int GetTotalNumMasterPartitions() => numCityPerSilo * basic.numSiloPerRegion * basic.numRegion;

    public void PrintContent(StreamWriter file)
    {
        basic.PrintContent(file);
        //file.WriteLine($"     EnvSetting: numCityPerSilo               = {numCityPerSilo}");
        //file.WriteLine($"     EnvSetting: numCustomerPerCity           = {numCustomerPerCity}");
        //file.WriteLine($"     EnvSetting: totalNumSeller               = {totalNumSeller}");
        //file.WriteLine($"     EnvSetting: numProductPerSeller          = {numProductPerSeller}");
        //file.WriteLine($"     EnvSetting: numRegionPerSeller           = {numRegionPerSeller}");
        //file.WriteLine($"     EnvSetting: numSiloPerSeller             = {numSiloPerSeller}");
        //file.WriteLine($"     EnvSetting: numCityPerSeller             = {numCityPerSeller}");
        //file.WriteLine($"     EnvSetting: numProductPerStockCity       = {numProductPerStockCity}");
        //file.WriteLine($"     EnvSetting: initialNumProductPerCart     = {initialNumProductPerCart}");
        //file.WriteLine($"     EnvSetting: maxPricePerProduct           = {maxPricePerProduct}");
        //file.WriteLine($"     EnvSetting: maxQuantityInStockPerProduct = {maxQuantityInStockPerProduct}");
    }
}
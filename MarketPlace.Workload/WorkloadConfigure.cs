using Experiment.Common;
using MessagePack;
using Utilities;

namespace MarketPlace.Workload;

[MessagePackObject(keyAsPropertyName: true)]
public class WorkloadConfigure : IWorkloadConfigure
{
    public readonly Dictionary<MarketPlaceTxnType, int> txnTypes;

    /// <summary> the percentage that a hot customer is selected </summary>
    public readonly int hotCustomerPercent;

    /// <summary> the number of hot customers among all customers in each city </summary>
    public readonly double hotCustomerPerCity;

    /// <summary> the percentage that a hot seller is selected </summary>
    public readonly int hotSellerPercent;

    /// <summary> the number of hot sellers among all sellers in each city </summary>
    public readonly double hotSellerPerCity;

    /// <summary> the percentage that a hot product is selected </summary>
    public readonly int hotProductPercent;

    /// <summary> the number of hot products among all products of each seller </summary>
    public readonly double hotProductPerSeller;

    public readonly int maxNumProductPerCheckout;

    public readonly int maxQuantityToBuyPerProduct;

    /// <summary> this is only for transactions that can be executed as ACT, like AddItemToCart </summary>
    public readonly int pactPercent;

    public readonly int multiSiloPercent;

    public readonly int actPipeSize;

    public readonly int pactPipeSize;

    public readonly int numActConsumer;

    public readonly int numPactConsumer;

    public int migrationPipeSize;

    public int numMigrationConsumer;

    public WorkloadConfigure(
        Dictionary<MarketPlaceTxnType, int> txnTypes, 

        int hotCustomerPercent,
        double hotCustomerPerCity,

        int hotSellerPercent, 
        double hotSellerPerCity, 

        int hotProductPercent,
        double hotProductPerSeller,

        int maxNumProductPerCheckout, 
        int maxQuantityToBuyPerProduct,

        int pactPercent,
        int multiSiloPercent,

        int actPipeSize, 
        int pactPipeSize, 
        int numActConsumer, 
        int numPactConsumer,
        
        int migrationPipeSize,
        int numMigrationConsumer)
    {
        this.txnTypes = txnTypes;

        this.hotCustomerPercent = hotCustomerPercent;
        this.hotCustomerPerCity = hotCustomerPerCity;

        this.hotSellerPercent = hotSellerPercent;
        this.hotSellerPerCity = hotSellerPerCity;

        this.hotProductPercent = hotProductPercent;
        this.hotProductPerSeller = hotProductPerSeller;

        this.maxNumProductPerCheckout = maxNumProductPerCheckout;
        this.maxQuantityToBuyPerProduct = maxQuantityToBuyPerProduct;

        this.pactPercent = pactPercent;
        this.multiSiloPercent = multiSiloPercent;

        this.actPipeSize = actPipeSize;
        this.pactPipeSize = pactPipeSize;
        this.numActConsumer = numActConsumer;
        this.numPactConsumer = numPactConsumer;

        this.migrationPipeSize = migrationPipeSize;
        this.numMigrationConsumer = numMigrationConsumer;
    }

    public WorkloadConfigure(
        Dictionary<MarketPlaceTxnType, int> txnTypes,

        int pactPercent,

        int actPipeSize,
        int pactPipeSize,
        int numActConsumer,
        int numPactConsumer,

        int migrationPipeSize,
        int numMigrationConsumer)
    { 
        this.txnTypes = txnTypes;

        this.pactPercent = pactPercent;

        this.actPipeSize = actPipeSize;
        this.pactPipeSize = pactPipeSize;
        this.numActConsumer = numActConsumer;
        this.numPactConsumer = numPactConsumer;

        this.migrationPipeSize = migrationPipeSize;
        this.numMigrationConsumer = numMigrationConsumer;
    }

    public int GetActPipeSize() => actPipeSize;

    public int GetPactPipeSize() => pactPipeSize;

    public int GetNumActConsumer() => numActConsumer;

    public int GetNumPactConsumer() => numPactConsumer;

    public int GetMigrationPipeSize() => migrationPipeSize;

    public int GetNumMigrationConsumer() => numMigrationConsumer;
}
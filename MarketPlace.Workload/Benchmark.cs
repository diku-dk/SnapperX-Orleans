using Concurrency.Common;
using Concurrency.Common.Cache;
using Concurrency.Interface.GrainPlacement;
using Experiment.Common;
using MarketPlace.Interfaces;
using Replication.Interface.GrainReplicaPlacement;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Workload;

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
            case ImplementationType.SNAPPERSIMPLE:
            case ImplementationType.SNAPPER:
                return NewSnapperTransaction(data, isReplica, false);
            case ImplementationType.SNAPPERFINE:
                return NewSnapperTransaction(data, isReplica, true);
            case ImplementationType.ORLEANSTXN: 
                return NewOrleansTransaction(data, isReplica);
            case ImplementationType.NONTXN: 
                return NewNonTransaction(data, isReplica);
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
                Debug.Assert(data.accessedKeysPerGrain != null);
                Debug.Assert(data.accessedKeysPerGrain.Count != 0);

                if (isFine) return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains, data.accessedKeysPerGrain);
                return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains);
            }
            else return pm.SubmitTransaction(data.grains.First(), startFunc);
        }
        else
        {
            var pm = replicaPmList[rnd.Next(0, replicaPmList.Count)];
            if (isDet) return pm.SubmitTransaction(data.grains.First(), startFunc, data.grains);
            else return pm.SubmitTransaction(data.grains.First(), startFunc);
        }
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
        else throw new NotImplementedException();
    }

    Task<TransactionResult> NewOrleansTransaction(RequestData data, bool isReplica)
    {
        var grainID = data.grains.First();
        var txnType = Enum.Parse<MarketPlaceTxnType>(data.transactionType);

        if (!isReplica)
        {
            switch (txnType)
            {
                case MarketPlaceTxnType.AddItemToCart: return GetGrainReference<IOrleansTransactionalCartActor>(grainID).AddItemToCart(data.funcInput);
                case MarketPlaceTxnType.DeleteItemInCart: return GetGrainReference<IOrleansTransactionalCartActor>(grainID).DeleteItemInCart(data.funcInput);
                case MarketPlaceTxnType.Checkout: return GetGrainReference<IOrleansTransactionalCartActor>(grainID).Checkout(data.funcInput);

                case MarketPlaceTxnType.UpdatePrice: return GetGrainReference<IOrleansTransactionalProductActor>(grainID).UpdatePrice(data.funcInput);
                case MarketPlaceTxnType.DeleteProduct: return GetGrainReference<IOrleansTransactionalProductActor>(grainID).DeleteProduct(data.funcInput);
                case MarketPlaceTxnType.AddProducts: return GetGrainReference<IOrleansTransactionalProductActor>(grainID).AddProducts(data.funcInput);

                case MarketPlaceTxnType.AddStock: return GetGrainReference<IOrleansTransactionalStockActor>(grainID).AddStock(data.funcInput);
                case MarketPlaceTxnType.ReplenishStock: return GetGrainReference<IOrleansTransactionalStockActor>(grainID).ReplenishStock(data.funcInput);

                default:
                    throw new Exception($"The transaction type {txnType} is not supported. ");
            }
        }
        else
        {
            switch (txnType)
            {
                case MarketPlaceTxnType.RefreshSellerDashboard:
                    throw new NotImplementedException();
                default:
                    throw new Exception($"The transaction type {txnType} is not supported. ");
            }
        }
    }

    Task<TransactionResult> NewNonTransaction(RequestData data, bool isReplica)
    {
        var grainID = data.grains.First();
        var txnType = Enum.Parse<MarketPlaceTxnType>(data.transactionType);

        if (!isReplica)
        {
            switch (txnType)
            {
                case MarketPlaceTxnType.AddItemToCart: return GetGrainReference<INonTransactionalSimpleCartActor>(grainID).AddItemToCart(data.funcInput);
                case MarketPlaceTxnType.DeleteItemInCart: return GetGrainReference<INonTransactionalSimpleCartActor>(grainID).DeleteItemInCart(data.funcInput);
                case MarketPlaceTxnType.Checkout: return GetGrainReference<INonTransactionalSimpleCartActor>(grainID).Checkout(data.funcInput);

                case MarketPlaceTxnType.UpdatePrice: return GetGrainReference<INonTransactionalSimpleProductActor>(grainID).UpdatePrice(data.funcInput);
                case MarketPlaceTxnType.DeleteProduct: return GetGrainReference<INonTransactionalSimpleProductActor>(grainID).DeleteProduct(data.funcInput);
                case MarketPlaceTxnType.AddProducts: return GetGrainReference<INonTransactionalSimpleProductActor>(grainID).AddProducts(data.funcInput);

                case MarketPlaceTxnType.AddStock: return GetGrainReference<INonTransactionalSimpleStockActor>(grainID).AddStock(data.funcInput);
                case MarketPlaceTxnType.ReplenishStock: return GetGrainReference<INonTransactionalSimpleStockActor>(grainID).ReplenishStock(data.funcInput);

                default:
                    throw new Exception($"The transaction type {txnType} is not supported. ");
            }
        }
        else
        {
            switch (txnType)
            {
                case MarketPlaceTxnType.RefreshSellerDashboard:
                    throw new NotImplementedException();
                default:
                    throw new Exception($"The transaction type {txnType} is not supported. ");
            }
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
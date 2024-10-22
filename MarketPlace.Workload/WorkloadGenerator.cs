using Concurrency.Common;
using Concurrency.Common.State;
using Experiment.Common;
using MarketPlace.Grains;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MathNet.Numerics.Distributions;
using Microsoft.CodeAnalysis;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Workload;

public class WorkloadGenerator : IWorkloadGenerator
{
    readonly string myRegion;
    readonly string mySilo;
    readonly int regionIndex;
    readonly int siloIndex;
    readonly int numEpoch;
    readonly WorkloadConfigure workload;
    readonly EnvSetting envSetting;
    readonly BasicEnvSetting basicEnvSetting;
    readonly EnvConfigure envConfigureHelper;
    readonly IDiscreteDistribution cityDistribution;
    readonly IDiscreteDistribution hotCustomerDistribution;
    readonly IDiscreteDistribution normalCustomerDistribution;
    readonly IDiscreteDistribution hotSellerDistribution;
    readonly IDiscreteDistribution normalSellerDistribution;
    readonly IDiscreteDistribution hotProductDistribution;
    readonly IDiscreteDistribution normalProductDistribution;
    readonly IDiscreteDistribution checkoutSizeDistribution;
    readonly IDiscreteDistribution multiSiloDistribution = new DiscreteUniform(0, 99, new Random());            // [0, 99]
    readonly IDiscreteDistribution hotCustomerPercentDistribution = new DiscreteUniform(0, 99, new Random());   // [0, 99]
    readonly IDiscreteDistribution hotSellerPercentDistribution = new DiscreteUniform(0, 99, new Random());     // [0, 99]
    readonly IDiscreteDistribution hotProductPercentDistribution = new DiscreteUniform(0, 99, new Random());    // [0, 99]
    readonly IDiscreteDistribution pactPercentDistribution = new DiscreteUniform(0, 99, new Random());          // [0, 99]
    readonly IDiscreteDistribution txnTypeDistribution = new DiscreteUniform(0, 99, new Random());
    readonly List<(MarketPlaceTxnType, int)> txnTypes;    // the percentage of different types of transactions
    readonly Random rnd = new Random();
    readonly Dictionary<string, string> nameMap;

    // ================================================================================================= for replication
    readonly bool isReplica;
    readonly int numReplicaSiloPerRegion;
    readonly List<string> localReplicaSiloList;
    readonly Dictionary<string, List<int>> replicaPartitionIDsPerSilo;    // silo ID => list of replica partitions

    public WorkloadGenerator(
        bool isReplica,
        string myRegion,
        string mySilo,
        int regionIndex,
        int siloIndex,
        IEnvSetting envSetting,
        IEnvConfigure envConfigureHelper,
        WorkloadConfigure workload,

        List<string> localReplicaSiloList)
    {
        this.myRegion = myRegion;
        this.mySilo = mySilo;
        this.regionIndex = regionIndex;
        this.siloIndex = siloIndex;
        this.envSetting = (EnvSetting)envSetting;
        this.basicEnvSetting = envSetting.GetBasic();
        this.envConfigureHelper = (EnvConfigure)envConfigureHelper;
        this.workload = workload;
        txnTypes = workload.txnTypes.Select(x => (x.Key, x.Value)).ToList();
        numEpoch = basicEnvSetting.GetNumEpochAndNumReRun().Item1;
        nameMap = new GrainNameHelper().GetNameMap(basicEnvSetting.implementationType);

        var numHotSellerPerCity = (int)(workload.hotSellerPerCity * this.envSetting.numSellerPerCity / 100.0);
        hotSellerDistribution = new DiscreteUniform(0, numHotSellerPerCity - 1, new Random());
        if (numHotSellerPerCity == this.envSetting.numSellerPerCity) normalSellerDistribution = hotSellerDistribution;
        else normalSellerDistribution = new DiscreteUniform(numHotSellerPerCity, this.envSetting.numSellerPerCity - 1, new Random());

        if (!isReplica)
        {
            var numHotCustomerPerCity = (int)(workload.hotCustomerPerCity * this.envSetting.numCustomerPerCity / 100.0);
            hotCustomerDistribution = new DiscreteUniform(0, numHotCustomerPerCity - 1, new Random());
            if (numHotCustomerPerCity == this.envSetting.numCustomerPerCity) normalCustomerDistribution = hotCustomerDistribution;
            else normalCustomerDistribution = new DiscreteUniform(numHotCustomerPerCity, this.envSetting.numCustomerPerCity - 1, new Random());

            var numHotProductPerSeller = (int)(workload.hotProductPerSeller * this.envSetting.numProductPerSeller / 100.0);
            hotProductDistribution = new DiscreteUniform(0, numHotProductPerSeller - 1, new Random());
            if (numHotProductPerSeller == this.envSetting.numProductPerSeller) normalProductDistribution = hotProductDistribution;
            else normalProductDistribution = new DiscreteUniform(numHotProductPerSeller, this.envSetting.numProductPerSeller - 1, new Random());

            checkoutSizeDistribution = new DiscreteUniform(1, this.workload.maxNumProductPerCheckout, new Random());
        }
        cityDistribution = new DiscreteUniform(0, this.envSetting.numCityPerSilo - 1, new Random());
        
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

        Console.WriteLine($"MarketPlace WorkloadGenerator: isReplica = {isReplica}, region index = {regionIndex}, silo index = {siloIndex}");
    }

    MarketPlaceTxnType GetTxnType()
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

    bool isPACT()
    {
        var sample = pactPercentDistribution.Sample();
        if (sample < workload.pactPercent) return true;
        else return false;
    }

    bool isHotCustomer()
    {
        var sample = hotCustomerPercentDistribution.Sample();
        if (sample < workload.hotCustomerPercent) return true;
        else return false;
    }

    /// <summary> determine whether a seller is selected from the hot set or not </summary>
    bool isHotSeller()
    {
        var sample = hotSellerPercentDistribution.Sample();
        if (sample < workload.hotSellerPercent) return true;
        else return false;
    }

    /// <summary> determine whether a product is selected from the hot set or not </summary>
    bool isHotProduct()
    {
        var sample = hotProductPercentDistribution.Sample();
        if (sample < workload.hotProductPercent) return true;
        else return false;
    }

    bool isMultiSilo()
    {
        var sample = multiSiloDistribution.Sample();
        if (sample < workload.multiSiloPercent) return true;
        else return false;
    }

    public Dictionary<int, Queue<(bool, RequestData)>> GenerateSimpleWorkload()
    {
        var numTxnPerEpoch = Constants.BASE_NUM_TRANSACTION;
        if (basicEnvSetting.implementationType == ImplementationType.NONTXN) numTxnPerEpoch *= 2;

        var shared_requests = new Dictionary<int, Queue<(bool, RequestData)>>();
        for (int epoch = 0; epoch < numEpoch; epoch++)
        {
            shared_requests.Add(epoch, new Queue<(bool, RequestData)>());
            for (int txn = 0; txn < numTxnPerEpoch; txn++)
            {
                var txnType = GetTxnType();
                switch (txnType)
                {
                    case MarketPlaceTxnType.AddItemToCart:
                        shared_requests[epoch].Enqueue(AddItemToCart());
                        break;
                    case MarketPlaceTxnType.DeleteItemInCart:
                        shared_requests[epoch].Enqueue(DeleteItemInCart());
                        break;
                    case MarketPlaceTxnType.AddProducts:
                        shared_requests[epoch].Enqueue(AddProducts());
                        break;
                    case MarketPlaceTxnType.UpdatePrice:
                        shared_requests[epoch].Enqueue(UpdatePrice());
                        break;
                    case MarketPlaceTxnType.DeleteProduct:
                        shared_requests[epoch].Enqueue(DeleteProduct());
                        break;
                    case MarketPlaceTxnType.Checkout:
                        shared_requests[epoch].Enqueue(Checkout());
                        break;
                    case MarketPlaceTxnType.AddStock:
                        shared_requests[epoch].Enqueue(AddStock());
                        break;
                    case MarketPlaceTxnType.ReplenishStock:
                        shared_requests[epoch].Enqueue(ReplenishStock());
                        break;
                    case MarketPlaceTxnType.RefreshSellerDashboard:
                        shared_requests[epoch].Enqueue(RefreshSellerDashboard());
                        break;
                    default:
                        throw new Exception($"The txn {txnType} is not supported for {BenchmarkType.MARKETPLACE} benchmark. ");
                }
            }
        }

        return shared_requests;
    }

    /// <summary> can be run as a PACT </summary>
    (bool, RequestData) AddItemToCart()
    {
        // randomly select a customer
        (var customerID, var cartActorID, _) = GetCart();

        // randomly select a product
        (var productID, var productActorID) = GetProduct();

        // need to access product actor to register reference
        var grains = new List<GrainID> { cartActorID, productActorID };  

        var accessedKeysPerGrain = new Dictionary<GrainID, HashSet<ISnapperKey>>
        {
            { cartActorID, new HashSet<ISnapperKey>{ productID } },
            { productActorID, new HashSet<ISnapperKey>{ productID }}
        };
        
        return (isPACT(), new RequestData(false, false, MarketPlaceTxnType.AddItemToCart.ToString(), grains, accessedKeysPerGrain, productID));
    }

    /// <summary> can be run as a PACT </summary>
    (bool, RequestData) DeleteItemInCart()
    {
        // randomly select a customer
        (var customerID, var cartActorID, _) = GetCart();

        // randomly select a product
        (var productID, var productActorID) = GetProduct();

        // need to access product actor to de-register reference
        var grains = new List<GrainID> { cartActorID, productActorID };

        var accessedKeysPerGrain = new Dictionary<GrainID, HashSet<ISnapperKey>>
        {
            { cartActorID, new HashSet<ISnapperKey>{ productID } },
            { productActorID, new HashSet<ISnapperKey>{ productID }}
        };

        return (isPACT(), new RequestData(false, false, MarketPlaceTxnType.DeleteItemInCart.ToString(), grains, accessedKeysPerGrain, productID));
    }

    /// <summary> can be run as both ACT or PACT </summary>
    (bool, RequestData) Checkout()
    {
        // randomly select a customer
        (var customerID, var cartActorID, var cityIndex) = GetCart();

        // randomly select a delivery city
        var multiSilo = isMultiSilo();
        var selectedSiloIndex = multiSilo ? (siloIndex + 1) % basicEnvSetting.numSiloPerRegion : siloIndex;
        var deliveryCityID = IdMapping.GetCityID(regionIndex, selectedSiloIndex, cityIndex, basicEnvSetting.numSiloPerRegion, envSetting.numCityPerSilo);
        var deliveryAddress = new Address(deliveryCityID, "fullAddress", "name", "contact");
        var paymentMethod = new PaymentMethod(customerID.id, "paymentService");

        // find all potential sellers that have stock in the specified delivery city
        var potentialSellers = new HashSet<int>();
        foreach (var item in envConfigureHelper.stockCitiesPerSeller)
            if (item.Value.Contains(deliveryCityID)) potentialSellers.Add(item.Key);

        // generate a list of items that have stock in the specified delivery city
        var items = new Dictionary<ProductID, int>();
        var numItem = checkoutSizeDistribution.Sample();
        while (items.Count < numItem)
        {
            (var productID, var grainID) = GetProduct(potentialSellers, deliveryCityID);
            if (!items.ContainsKey(productID)) items.Add(productID, rnd.Next(0, workload.maxQuantityToBuyPerProduct));
        }

        var checkout = new Checkout(deliveryAddress, paymentMethod, items);

        (var accessedActors, var accessedKeysPerActor) = GetActorAccessInfo(cartActorID, checkout);

        // the accessed silos and regions wil be calculated after the trasnaction is finished
        return (isPACT(), new RequestData(multiSilo, false, MarketPlaceTxnType.Checkout.ToString(), accessedActors, accessedKeysPerActor, checkout));
    }

    /// <summary> the update on a product will forward to other grains, so cannot run as PACT, also cannot determine if it's multi-silo or multi-region </summary>
    (bool, RequestData) UpdatePrice()
    {
        (var productID, var productActorID) = GetProduct();
        var newPrice = (double)rnd.Next(0, envSetting.maxPricePerProduct);
        return (false, new RequestData(false, false, MarketPlaceTxnType.UpdatePrice.ToString(), new List<GrainID> { productActorID }, null, (productID, newPrice)));
    }

    /// <summary> the deletion of a prodcut will cause the deletion of keys on other grains, so cannot run as PACT </summary>
    (bool, RequestData) DeleteProduct()
    {
        (var productID, var productActorID) = GetProduct();
        return (false, new RequestData(false, false, MarketPlaceTxnType.DeleteProduct.ToString(), new List<GrainID> { productActorID }, null, productID));
    }

    /// <summary> add a new product to the product actor, run an ACT because it only accesses one actor </summary>
    (bool, RequestData) AddProducts()
    {
        (var productID, var productActorID) = GetProduct();
        var price = rnd.Next(0, envSetting.maxPricePerProduct);
        var productInfo = new ProductInfo("name", price);
        var input = new Dictionary<ProductID, ProductInfo> { { productID, productInfo} };

        var accessedActors = new List<GrainID> { productActorID };
        var accessedKeysPerActor = new Dictionary<GrainID, HashSet<ISnapperKey>> { { productActorID, new HashSet<ISnapperKey> { productID } } };

        return (isPACT(), new RequestData(false, false, MarketPlaceTxnType.AddProducts.ToString(), accessedActors, accessedKeysPerActor, input));
    }

    /// <summary> replenish stock to an existing product, run an ACT because it only accesses one actor </summary>
    (bool, RequestData) ReplenishStock()
    {
        // randomly select a seller and a product
        (var productID, var productActorID) = GetProduct();
        var sellerID = productID.sellerID.id;
        var quantity = 100 * rnd.Next(0, workload.maxQuantityToBuyPerProduct);

        // randomly select a stock city
        var stockCities = envConfigureHelper.stockCitiesPerSeller[sellerID];
        Debug.Assert(stockCities.Count == envSetting.numStockCityPerSeller);
        var cityID = stockCities.ToList()[rnd.Next(0, envSetting.numStockCityPerSeller)];

        // get the target stock actor ID
        var stockActorGuid = IdMapping.GetStockActorGuid(sellerID, cityID);
        var stockActorID = new GrainID(stockActorGuid, nameMap["StockActor"]);

        var accessedActors = new List<GrainID> { stockActorID };
        var accessedKeysPerActor = new Dictionary<GrainID, HashSet<ISnapperKey>>{ { stockActorID, new HashSet<ISnapperKey> { productID } } };

        return (isPACT(), new RequestData(false, false, MarketPlaceTxnType.ReplenishStock.ToString(), accessedActors, accessedKeysPerActor, (productID, quantity)));
    }

    /// <summary> add stock to a newly created product, can run as a PACT </summary>
    (bool, RequestData) AddStock()
    {
        (var productID, var productActorID) = GetProduct();
        var sellerID = productID.sellerID.id;
        var quantity = 100 * rnd.Next(0, workload.maxQuantityToBuyPerProduct);

        // randomly select a stock city
        var stockCities = envConfigureHelper.stockCitiesPerSeller[sellerID];
        Debug.Assert(stockCities.Count == envSetting.numStockCityPerSeller);
        var cityID = stockCities.ToList()[rnd.Next(0, envSetting.numStockCityPerSeller)];

        // get the target stock actor ID
        var stockActorGuid = IdMapping.GetStockActorGuid(sellerID, cityID);
        var stockActorID = new GrainID(stockActorGuid, nameMap["StockActor"]);
        var input = (productID, new StockInfo(quantity, "default"));

        var accessedActors = new List<GrainID> { stockActorID, productActorID };
        var accessedKeysPerActor = new Dictionary<GrainID, HashSet<ISnapperKey>> 
        { 
            { stockActorID, new HashSet<ISnapperKey>{ productID } },
            { productActorID, new HashSet<ISnapperKey>{ productID } }
        };

        return (isPACT(), new RequestData(false, false, MarketPlaceTxnType.AddStock.ToString(), accessedActors, accessedKeysPerActor, input));
    }

    /// <summary> if it is a PACT, must be single-silo </summary>
    (bool, RequestData) RefreshSellerDashboard()
    {
        // randomly select a seller
        var sellerID = GetSellerID();
        var sellerActorGuid = IdMapping.GetSellerActorGuid(sellerID, envConfigureHelper.baseCityPerSeller[sellerID]);
        var sellerActorID = new GrainID(sellerActorGuid, nameMap["ReplicatedSellerActor"]);
        var grains = new List<GrainID> { sellerActorID };

        // find all stock cities located in one replica silo
        var replicaSiloID = localReplicaSiloList[siloIndex];
        var cities = replicaPartitionIDsPerSilo[replicaSiloID].ToHashSet();
        var stockCities = envConfigureHelper.stockCitiesPerSeller[sellerID];
        var selectedStockCities = stockCities.Where(x => cities.Contains(x)).ToList();
        
        // calculate the stock actor for each stock city
        var stockActorIDs = new List<GrainID>();
        foreach (var cityID in selectedStockCities)
        {
            var stockActorGuid = IdMapping.GetStockActorGuid(sellerID, cityID);
            var stockActorID = new GrainID(stockActorGuid, nameMap["ReplicatedStockActor"]);
            stockActorIDs.Add(stockActorID);
            grains.Add(stockActorID);
        }

        return (isPACT(), new RequestData(false, false, MarketPlaceTxnType.RefreshSellerDashboard.ToString(), grains, null, stockActorIDs));
    }

    /// <returns> product ID, product actor ID </returns>
    (ProductID, GrainID) GetProduct()
    {
        // randomly select a product
        var sellerID = GetSellerID();
        var productID = isHotProduct() ? hotProductDistribution.Sample() : normalProductDistribution.Sample();
        var baseCityID = envConfigureHelper.baseCityPerSeller[sellerID];
        var productActorGuid = IdMapping.GetProductActorGuid(sellerID, baseCityID);
        var productActorID = new GrainID(productActorGuid, nameMap["ProductActor"]);

        return (new ProductID(new SellerID(sellerID, baseCityID), productID), productActorID);
    }

    /// <returns> product ID, product actor ID, select a product from the given set of sellers </returns>
    (ProductID, GrainID) GetProduct(HashSet<int> potentialSellers, int deliveryCityID)
    {
        // randomly select a seller from the given set of sellers
        Debug.Assert(potentialSellers.Count == envSetting.numSellerPerCity);
        var sorted = potentialSellers.ToList();
        sorted.Sort();

        var sellerIndex = isHotSeller() ? hotSellerDistribution.Sample() : normalSellerDistribution.Sample();
        Debug.Assert(sellerIndex < envSetting.numSellerPerCity);
        var sellerID = sorted[sellerIndex];

        // randomly select a product
        var productID = isHotProduct() ? hotProductDistribution.Sample() : normalProductDistribution.Sample();
        var baseCityID = envConfigureHelper.baseCityPerSeller[sellerID];
        var productActorGuid = IdMapping.GetProductActorGuid(sellerID, baseCityID);
        var productActorID = new GrainID(productActorGuid, nameMap["ProductActor"]);

        return (new ProductID(new SellerID(sellerID, baseCityID), productID), productActorID);
    }

    /// <returns> customer ID, cart actor ID </returns>
    (CustomerID, GrainID, int) GetCart()
    {
        // randomly select a customer
        var cityIndex = cityDistribution.Sample();
        var baseCityID = IdMapping.GetCityID(regionIndex, siloIndex, cityIndex, basicEnvSetting.numSiloPerRegion, envSetting.numCityPerSilo);
        var customerID = isHotCustomer() ? hotCustomerDistribution.Sample() : normalCustomerDistribution.Sample();
        var cartActorGuid = IdMapping.GetCartActorGuid(customerID, baseCityID);
        var cartActorID = new GrainID(cartActorGuid, nameMap["CartActor"]);

        return (new CustomerID(customerID, baseCityID), cartActorID, cityIndex);
    }

    int GetSellerID()
    {
        var cityIndex = cityDistribution.Sample();
        var sellerIndex = isHotSeller() ? hotSellerDistribution.Sample() : normalSellerDistribution.Sample();
        var sellerID = IdMapping.GetSellerID(siloIndex, cityIndex, sellerIndex, basicEnvSetting.numSiloPerRegion, envSetting.numCityPerSilo, envSetting.numSellerPerCity);
        return sellerID;
    }

    (List<GrainID>, Dictionary<GrainID, HashSet<ISnapperKey>>) GetActorAccessInfo(GrainID cartActorID, Checkout checkout)
    {
        var accessedActors = new List<GrainID> { cartActorID };
        var accessedKeysPerActor = new Dictionary<GrainID, HashSet<ISnapperKey>> { { cartActorID, new HashSet<ISnapperKey>() } };
        
        // STEP 1: calculate the accessed stock actor (per seller)
        var accessedStockActors = new HashSet<GrainID>();
        var deliveryCityID = checkout.deliveryAddress.cityID;  // only check the stock actors in the delivery city
        foreach (var item in checkout.items)
        {
            var sellerID = item.Key.sellerID;
            var stockActorGuid = IdMapping.GetStockActorGuid(sellerID.id, deliveryCityID);
            var stockActorID = new GrainID(stockActorGuid, nameMap["StockActor"]);
            accessedStockActors.Add(stockActorID);

            if (!accessedKeysPerActor.ContainsKey(stockActorID)) accessedKeysPerActor.Add(stockActorID, new HashSet<ISnapperKey>());
            accessedKeysPerActor[stockActorID].Add(item.Key);

            accessedKeysPerActor[cartActorID].Add(item.Key);
        }
        accessedActors.AddRange(accessedStockActors);

        // STEP 2: add the payment actor (per customer)
        (var customerID, var baseCityID) = Helper.ConvertGuidToTwoInts(cartActorID.id);
        var paymentActorGuid = IdMapping.GetPaymentActorGuid(customerID, baseCityID);
        var paymentActorID = new GrainID(paymentActorGuid, nameMap["PaymentActor"]);
        accessedActors.Add(paymentActorID);
        accessedKeysPerActor.Add(paymentActorID, new HashSet<ISnapperKey> { { new CustomerID(customerID, baseCityID) } });

        // STEP 3: add order actor (per customer)
        var orderActorGuid = IdMapping.GetOrderActorGuid(customerID, baseCityID);
        var orderActorID = new GrainID(orderActorGuid, nameMap["OrderActor"]);
        accessedActors.Add(orderActorID);
        accessedKeysPerActor.Add(orderActorID, new HashSet<ISnapperKey> { new GeneralStringKey("nextOrderID") });

        // STEP 4: add shipment actor (per seller)
        var accessedShipmentActors = new HashSet<GrainID>();
        foreach (var item in checkout.items)
        {
            var sellerID = item.Key.sellerID;
            var shipmentActorGuid = IdMapping.GetShipmentActorGuid(sellerID.id, deliveryCityID);
            var shipmentActorID = new GrainID(shipmentActorGuid, nameMap["ShipmentActor"]);
            accessedShipmentActors.Add(shipmentActorID);

            if (!accessedKeysPerActor.ContainsKey(shipmentActorID)) 
                accessedKeysPerActor.Add(shipmentActorID, new HashSet<ISnapperKey> { new GeneralStringKey("nextPackageID"), sellerID });
        }
        accessedActors.AddRange(accessedShipmentActors);

        // STEP 5: add seller actor
        var accessedSellerActors = new HashSet<GrainID>();
        foreach (var item in checkout.items)
        {
            var sellerID = item.Key.sellerID;
            var sellerActorGuid = IdMapping.GetSellerActorGuid(sellerID.id, sellerID.baseCityID);
            var sellerActorID = new GrainID(sellerActorGuid, nameMap["SellerActor"]);
            accessedSellerActors.Add(sellerActorID);

            if (!accessedKeysPerActor.ContainsKey(sellerActorID)) accessedKeysPerActor.Add(sellerActorID, new HashSet<ISnapperKey>());
            accessedKeysPerActor[sellerActorID].Add(sellerID);
        }
        accessedActors.AddRange(accessedSellerActors);

        // STEP 6: add product actor
        var accessedProductActors = new HashSet<GrainID>();
        foreach(var item in checkout.items) 
        {
            var productID = item.Key;
            var productActorGuid = IdMapping.GetProductActorGuid(productID.sellerID.id, productID.sellerID.baseCityID);
            var productActorID = new GrainID(productActorGuid, nameMap["ProductActor"]);
            accessedProductActors.Add(productActorID);

            if (!accessedKeysPerActor.ContainsKey(productActorID)) accessedKeysPerActor.Add(productActorID, new HashSet<ISnapperKey>());
            accessedKeysPerActor[productActorID].Add(productID);
        }
        accessedActors.AddRange(accessedProductActors);

        Debug.Assert(accessedActors.Count == accessedKeysPerActor.Count);
        foreach (var item in accessedKeysPerActor) Debug.Assert(item.Value.Count != 0);
        
        return (accessedActors, accessedKeysPerActor);
    }
}
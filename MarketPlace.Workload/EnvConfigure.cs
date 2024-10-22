using Concurrency.Common;
using Concurrency.Common.Cache;
using Concurrency.Common.ICache;
using Concurrency.Common.State;
using Concurrency.Interface.DataModel;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Experiment.Common;
using MarketPlace.Grains;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.NonTransactionalKeyValueGrains;
using MarketPlace.Grains.SnapperTransactionalFineGrains;
using MarketPlace.Grains.SnapperTransactionalKeyValueGrains;
using MarketPlace.Grains.SnapperTransactionalSimpleGrains;
using MarketPlace.Grains.ValueState;
using MarketPlace.Interfaces;
using MarketPlace.Interfaces.ISnapperTransactionalGrains;
using MathNet.Numerics.Distributions;
using MessagePack;
using Replication.Interface.GrainReplicaPlacement;
using Replication.Interface.TransactionReplication;
using System.Diagnostics;
using System.Reflection;
using Utilities;

namespace MarketPlace.Workload;

[MessagePackObject]
public class EnvConfigure : IEnvConfigure
{
    [Key(0)]
    public Dictionary<GrainID, int> grainIDToPartitionID;                                            // grain ID => partition ID

    [Key(1)]
    public Dictionary<int, (string, string)> partitionIDToMasterInfo;                                // partition ID => (master region ID, master silo ID)

    [Key(2)]
    public Dictionary<GrainID, int> replicaGrainIDToPartitionID;                                     // replica grain ID => partition ID

    [Key(3)]
    public Dictionary<string, Dictionary<int, string>> partitionIDToReplicaInfo;                     // region ID, partition ID => replica silo ID in the region

    [Key(4)]
    public Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker;                             // grain ID => migration worker (guid, location) 

    /// <summary> each seller selects one city as its base, and uses this city to store the product list info </summary>
    [Key(5)]
    public Dictionary<int, int> baseCityPerSeller;                                                   // seller ID, base city ID

    [Key(6)]
    public Dictionary<int, HashSet<int>> stockCitiesPerSeller;                                       // seller ID, the cities where this seller has stock in

    Dictionary<string, Dictionary<string, Dictionary<string, List<GrainID>>>> grains;                // region ID, silo ID, grain class name, grains

    Dictionary<string, Dictionary<string, Dictionary<string, List<GrainID>>>> replicaGrains;         // region ID, silo ID, grain class name, replica grains

    Random rnd;

    public EnvConfigure() 
    {
        grainIDToPartitionID = new Dictionary<GrainID, int>();
        partitionIDToMasterInfo = new Dictionary<int, (string, string)>();
        replicaGrainIDToPartitionID = new Dictionary<GrainID, int>();
        partitionIDToReplicaInfo = new Dictionary<string, Dictionary<int, string>>();
        grainIDToMigrationWorker = new Dictionary<GrainID, (Guid, string)>();

        baseCityPerSeller = new Dictionary<int, int>();
        stockCitiesPerSeller = new Dictionary<int, HashSet<int>>();
        
        grains = new Dictionary<string, Dictionary<string, Dictionary<string, List<GrainID>>>>();
        replicaGrains = new Dictionary<string, Dictionary<string, Dictionary<string, List<GrainID>>>>();

        rnd = new Random();
    }

    public EnvConfigure(
        Dictionary<GrainID, int> grainIDToPartitionID, 
        Dictionary<int, (string, string)> partitionIDToMasterInfo, 
        Dictionary<GrainID, int> replicaGrainIDToPartitionID, 
        Dictionary<string, Dictionary<int, string>> partitionIDToReplicaInfo,
        Dictionary<GrainID, (Guid, string)> grainIDToMigrationWorker,
        Dictionary<int, int> baseCityPerSeller, 
        Dictionary<int, HashSet<int>> stockCitiesPerSeller)
    {
        this.grainIDToPartitionID = grainIDToPartitionID;
        this.partitionIDToMasterInfo = partitionIDToMasterInfo;
        this.replicaGrainIDToPartitionID = replicaGrainIDToPartitionID;
        this.partitionIDToReplicaInfo = partitionIDToReplicaInfo;
        this.grainIDToMigrationWorker = grainIDToMigrationWorker;
        this.baseCityPerSeller = baseCityPerSeller;
        this.stockCitiesPerSeller = stockCitiesPerSeller;
        grains = new Dictionary<string, Dictionary<string, Dictionary<string, List<GrainID>>>>();
        replicaGrains = new Dictionary<string, Dictionary<string, Dictionary<string, List<GrainID>>>>();
        rnd = new Random();
    }

    /// <summary> For MarketPlace, the partitionID is cityID </summary>
    public void GenerateGrainPlacementInfo(List<string> regionList, IEnvSetting iEnvSetting, StaticClusterInfo staticClusterInfo)
    {
        var envSetting = (EnvSetting)iEnvSetting;
        var basicEnvSetting = envSetting.GetBasic();
        var nameMap = new GrainNameHelper().GetNameMap(basicEnvSetting.implementationType);
        
        var regionDistribution = new DiscreteUniform(0, basicEnvSetting.numRegion - 1, new Random());        // [0, numRegion - 1]
        var siloDistribution = new DiscreteUniform(0, basicEnvSetting.numSiloPerRegion - 1, new Random());   // [0, numSiloPerRegion - 1]
        var cityDistribution = new DiscreteUniform(0, envSetting.numCityPerSilo - 1, new Random());          // [0, numCityPerLocalSilo - 1]
        
        MethodInfo? method;

        // for each seller, select a set of cities for storing stock
        for (var siloIndex = 0; siloIndex < basicEnvSetting.numSiloPerRegion; siloIndex++)
        {
            for (var cityIndex = 0; cityIndex < envSetting.numCityPerSilo; cityIndex++)
            {
                for (var sellerIndex = 0; sellerIndex < envSetting.numSellerPerCity; sellerIndex++)
                {
                    var cityID = IdMapping.GetCityID(0, siloIndex, cityIndex, basicEnvSetting.numSiloPerRegion, envSetting.numCityPerSilo);
                    var sellerID = IdMapping.GetSellerID(siloIndex, cityIndex, sellerIndex, basicEnvSetting.numSiloPerRegion, envSetting.numCityPerSilo, envSetting.numSellerPerCity);

                    var selectedCityIDs = new HashSet<int> { cityID };
                    Debug.Assert(selectedCityIDs.Count == envSetting.numStockCityPerSeller);
                    stockCitiesPerSeller.Add(sellerID, selectedCityIDs);

                    // select one city of these cities as base
                    var baseCityID = selectedCityIDs.ToList()[rnd.Next(0, selectedCityIDs.Count)];
                    baseCityPerSeller.Add(sellerID, baseCityID);
                    var productActorGuid = IdMapping.GetProductActorGuid(sellerID, baseCityID);
                    var productActorID = new GrainID(productActorGuid, nameMap["ProductActor"]);
                    grainIDToPartitionID.Add(productActorID, baseCityID);

                    var sellerActorGuid = IdMapping.GetSellerActorGuid(sellerID, baseCityID);
                    var sellerActorID = new GrainID(sellerActorGuid, nameMap["SellerActor"]);
                    grainIDToPartitionID.Add(sellerActorID, baseCityID);

                    // init stock and shipment actor in each stock city
                    foreach (var cID in selectedCityIDs)
                    {
                        var stockActorGuid = IdMapping.GetStockActorGuid(sellerID, cID);
                        var stockActorID = new GrainID(stockActorGuid, nameMap["StockActor"]);
                        grainIDToPartitionID.Add(stockActorID, cID);

                        var shipmentActorGuid = IdMapping.GetShipmentActorGuid(sellerID, cID);
                        var shipmentActorID = new GrainID(shipmentActorGuid, nameMap["ShipmentActor"]);
                        grainIDToPartitionID.Add(shipmentActorID, cID);
                    }
                }
            }
        }

        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];

            for (var siloIndex = 0; siloIndex < basicEnvSetting.numSiloPerRegion; siloIndex++)
            {
                var siloID = staticClusterInfo.localSiloListPerRegion[regionID][siloIndex];

                for (var cityIndex = 0; cityIndex < envSetting.numCityPerSilo; cityIndex++)
                {
                    var baseCityID = regionIndex * basicEnvSetting.numSiloPerRegion * envSetting.numCityPerSilo +
                                                                      siloIndex * envSetting.numCityPerSilo +
                                                                                  cityIndex;
                    partitionIDToMasterInfo.Add(baseCityID, (regionID, siloID));

                    for (var customerID = 0; customerID < envSetting.numCustomerPerCity; customerID++)
                    {
                        var customerGuid = IdMapping.GetCustomerActorGuid(customerID, baseCityID);
                        
                        var customerActorID = new GrainID(customerGuid, nameMap["CustomerActor"]);
                        var cartActorID = new GrainID(customerGuid, nameMap["CartActor"]);
                        var paymentActorID = new GrainID(customerGuid, nameMap["PaymentActor"]);
                        var orderActorID = new GrainID(customerGuid, nameMap["OrderActor"]);

                        grainIDToPartitionID.Add(customerActorID, baseCityID);
                        grainIDToPartitionID.Add(cartActorID, baseCityID);
                        grainIDToPartitionID.Add(paymentActorID, baseCityID);
                        grainIDToPartitionID.Add(orderActorID, baseCityID);
                    }
                }
            }
        }

        // check correctness
        var totalNumCustomer = basicEnvSetting.numRegion * basicEnvSetting.numSiloPerRegion * envSetting.numCityPerSilo * envSetting.numCustomerPerCity;
        var totalNumSeller = envSetting.numSellerPerCity * envSetting.numCityPerSilo * basicEnvSetting.numSiloPerRegion;
        var expected = new Dictionary<string, int>
        {
            { nameMap["CustomerActor"], totalNumCustomer },
            { nameMap["CartActor"], totalNumCustomer },
            { nameMap["PaymentActor"], totalNumCustomer },
            { nameMap["OrderActor"], totalNumCustomer },

            { nameMap["SellerActor"], totalNumSeller },
            { nameMap["ProductActor"], totalNumSeller },
            { nameMap["StockActor"], totalNumSeller * envSetting.numStockCityPerSeller },
            { nameMap["ShipmentActor"], totalNumSeller }
        };

        Console.WriteLine($"Generate {grainIDToPartitionID.Count} grain placement info. ");
        foreach (var item in expected) Console.WriteLine($"{item.Key}: {item.Value}");
        
        Debug.Assert(grainIDToPartitionID.Count == expected.Values.Sum());

        var actual = new Dictionary<string, MyCounter>();
        foreach (var item in grainIDToPartitionID)
        {
            if (!actual.ContainsKey(item.Key.className)) actual.Add(item.Key.className, new MyCounter());
            actual[item.Key.className].Add(1);
        }
        foreach (var item in actual) Debug.Assert(item.Value.Get() == expected[item.Key]);

        // assign each grain a migration worker
        var mwList = staticClusterInfo.mwList;
        foreach (var item in grainIDToPartitionID)
        {
            // select a migration worker in the same region and silo
            (var regionID, var siloID) = partitionIDToMasterInfo[item.Value];
            
            Debug.Assert(mwList.ContainsKey(regionID) && mwList[regionID].ContainsKey(siloID));
            var list = mwList[regionID][siloID];
            var selectedGuid = list[rnd.Next(0, list.Count)];
            grainIDToMigrationWorker.Add(item.Key, (selectedGuid, regionID + "+" + siloID));
        }
    }

    public void GenerateReplicaGrainPlacementInfo(List<string> regionList, IEnvSetting iEnvSetting, StaticReplicaInfo staticReplicaInfo)
    {
        throw new NotImplementedException();
        /*
        var envSetting = (EnvSetting)iEnvSetting;
        var basicEnvSetting = envSetting.GetBasic();
        var nameMap = new GrainNameHelper().GetNameMap(basicEnvSetting.implementationType);

        for (var sellerID = 0; sellerID < envSetting.totalNumSeller; sellerID++)
        {
            var baseCityID = baseCityPerSeller[sellerID];
            var sellerActorID = IdMapping.GetSellerActorGuid(sellerID, baseCityPerSeller[sellerID]);
            var grainID = new GrainID(sellerActorID, nameMap["ReplicatedSellerActor"]);
            replicaGrainIDToPartitionID.Add(grainID, baseCityID);

            foreach (var cityID in stockCitiesPerSeller[sellerID])
            {
                var replicaStockActorGuid = IdMapping.GetStockActorGuid(sellerID, cityID);
                var grainID1 = new GrainID(replicaStockActorGuid, nameMap["ReplicatedStockActor"]);
                replicaGrainIDToPartitionID.Add(grainID1, baseCityID);
                
                var replicaShipmentActorGuid = IdMapping.GetShipmentActorGuid(sellerID, cityID);
                var grainID2 = new GrainID(replicaShipmentActorGuid, nameMap["ReplicatedShipmentActor"]);
                replicaGrainIDToPartitionID.Add(grainID2, baseCityID);
            }
        }

        var numCityPerReplicaSilo = (basicEnvSetting.numSiloPerRegion * envSetting.numCityPerSilo) / basicEnvSetting.numReplicaSiloPerRegion;
        for (var regionIndex = 0; regionIndex < regionList.Count; regionIndex++)
        {
            var regionID = regionList[regionIndex];
            partitionIDToReplicaInfo.Add(regionID, new Dictionary<int, string>());

            for (var siloIndex = 0; siloIndex < basicEnvSetting.numSiloPerRegion; siloIndex++)
            {
                for (var cityIndex = 0; cityIndex < envSetting.numCityPerSilo; cityIndex++)
                {
                    var cityID = IdMapping.GetCityID(regionIndex, siloIndex, cityIndex, basicEnvSetting.numSiloPerRegion, envSetting.numCityPerSilo);

                    var replicaSiloList = staticReplicaInfo.localReplicaSiloListPerRegion[regionID];
                    var replicaSiloIndex = cityID / numCityPerReplicaSilo;
                    partitionIDToReplicaInfo[regionID].Add(cityID, replicaSiloList[replicaSiloIndex]);
                }
            }
        }

        // check correctness
        var expected = new Dictionary<string, int>
        {
            { nameMap["ReplicatedSellerActor"], envSetting.totalNumSeller },
            { nameMap["ReplicatedStockActor"], envSetting.totalNumSeller * envSetting.numCityPerSeller },
            { nameMap["ReplicatedShipmentActor"], envSetting.totalNumSeller * envSetting.numCityPerSeller },
        };

        Console.WriteLine($"Generate {replicaGrainIDToPartitionID.Count} replica grain placement info. ");
        foreach (var item in expected) Console.WriteLine($"{item.Key}: {item.Value}");

        Debug.Assert(replicaGrainIDToPartitionID.Count == expected.Values.Sum());

        var actual = new Dictionary<string, MyCounter>();
        foreach (var item in replicaGrainIDToPartitionID)
        {
            if (!actual.ContainsKey(item.Key.className)) actual.Add(item.Key.className, new MyCounter());
            actual[item.Key.className].Add(1);
        }
        foreach (var item in actual) Debug.Assert(item.Value.Get() == expected[item.Key]);
        */
    }

    /// <returns> cityID, regionID, siloID </returns>
    public (int, string, string) GetActorLocation(GrainID actorID)
    {
        Debug.Assert(grainIDToPartitionID.ContainsKey(actorID));
        var cityID = grainIDToPartitionID[actorID];
        (var regionID, var siloID) = partitionIDToMasterInfo[cityID];
        return (cityID, regionID, siloID);
    }

    public (Dictionary<GrainID, int>, Dictionary<int, (string, string)>, Dictionary<GrainID, (Guid, string)>) GetGrainPlacementInfo() => (grainIDToPartitionID, partitionIDToMasterInfo, grainIDToMigrationWorker);

    public (Dictionary<GrainID, int>, Dictionary<string, Dictionary<int, string>>) GetReplicaGrainPlacementInfo() => (replicaGrainIDToPartitionID, partitionIDToReplicaInfo);

    public async Task InitAllGrains(string myRegion, string mySilo, IEnvSetting iEnvSetting, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory)
    {
        var envSetting = (EnvSetting)iEnvSetting;
        var basicEnvSetting = envSetting.GetBasic();
        var implementationType = basicEnvSetting.implementationType;
        var nameMap = new GrainNameHelper().GetNameMap(implementationType);

        var sellerDistribution = new DiscreteUniform(0, envSetting.numSellerPerCity - 1, new Random());
        var productDistribution = new DiscreteUniform(0, envSetting.numProductPerSeller - 1, new Random());  // [0, numProductPerSeller - 1]
        
        if (!grains.ContainsKey(myRegion)) grains.Add(myRegion, new Dictionary<string, Dictionary<string, List<GrainID>>>());
        if (!grains[myRegion].ContainsKey(mySilo)) grains[myRegion].Add(mySilo, new Dictionary<string, List<GrainID>>());

        var grainsInSilo = new Dictionary<string, List<GrainID>>();
        foreach (var item in grainIDToPartitionID)
        {
            var actorID = item.Key;
            var partitionID = item.Value;
            (var regionID, var siloID) = partitionIDToMasterInfo[partitionID];
            if (regionID != myRegion || siloID != mySilo) continue;

            Debug.Assert(actorID.className != null);
            if (!grainsInSilo.ContainsKey(actorID.className)) grainsInSilo.Add(actorID.className, new List<GrainID>());
            grainsInSilo[actorID.className].Add(actorID);
        }
        grains[myRegion][mySilo] = grainsInSilo;

        var batchSize = implementationType == ImplementationType.ORLEANSTXN ? 10 : 100;
        var tasks = new List<Task>();
        if (implementationType == ImplementationType.SNAPPER || 
            implementationType == ImplementationType.SNAPPERSIMPLE || 
            implementationType == ImplementationType.SNAPPERFINE)
        {
            // activate all grains first (set underMigration as false)
            foreach (var item in grainsInSilo)
            {
                var actorType = item.Key;
                foreach (var grainID in item.Value)
                {
                    var grain = grainFactory.GetGrain<ITransactionExecutionGrain>(grainID.id, myRegion + "+" + mySilo, grainID.className);
                    tasks.Add(grain.ActivateGrain());

                    if (tasks.Count % batchSize == 0)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
                await Task.WhenAll(tasks);
                tasks.Clear();

                var strs = actorType.Split(".");
                Console.WriteLine($"Activate {item.Value.Count} {strs.Last()} actors. ");
            }
        }
        
        // init all grains
        var myPMListPerSilo = snapperClusterCache.GetPMList(myRegion);
        foreach (var item in grainsInSilo)
        {
            var actorType = item.Key;
            foreach (var grainID in item.Value)
            {
                object? input = null;
                if (actorType.Contains("ShipmentActor") || actorType.Contains("StockActor"))
                {
                    (var sellerID, var cityID) = Helper.ConvertGuidToTwoInts(grainID.id);
                    var baseCityID = baseCityPerSeller[sellerID];

                    input = baseCityID;
                }

                var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                var startFunc = new FunctionCall(GrainNameHelper.GetMethod(implementationType, actorType, "Init"), input);

                switch (implementationType)
                {
                    case ImplementationType.NONTXNKV:
                        tasks.Add(pm.SubmitNonTransactionalRequest(grainID, startFunc));
                        break;
                    case ImplementationType.SNAPPER:
                    case ImplementationType.SNAPPERSIMPLE:
                        tasks.Add(pm.SubmitTransaction(grainID, startFunc, new List<GrainID> { grainID }));
                        break;
                    case ImplementationType.SNAPPERFINE:
                        var accessedKeysPerActor = new Dictionary<GrainID, HashSet<ISnapperKey>>();
                        if (actorType.Contains("CartActor") ||
                            actorType.Contains("CustomerActor") ||
                            actorType.Contains("PaymentActor"))
                        {
                            (var customerID, var baseCityID) = Helper.ConvertGuidToTwoInts(grainID.id);
                            var id = new CustomerID(customerID, baseCityID);
                            accessedKeysPerActor.Add(grainID, new HashSet<ISnapperKey> { id });
                        }
                        else if (actorType.Contains("OrderActor"))
                        {
                            var nextOrderIDKey = new GeneralStringKey("nextOrderID");
                            accessedKeysPerActor.Add(grainID, new HashSet<ISnapperKey> { nextOrderIDKey });
                        }
                        else if (actorType.Contains("ProductActor"))
                        {
                            (var sellerID, var baseCityID) = Helper.ConvertGuidToTwoInts(grainID.id);
                            var id = new SellerID(sellerID, baseCityID);
                            accessedKeysPerActor.Add(grainID, new HashSet<ISnapperKey> { id });
                        }
                        else if (actorType.Contains("SellerActor"))
                        {
                            (var sellerID, var baseCityID) = Helper.ConvertGuidToTwoInts(grainID.id);
                            var id = new SellerID(sellerID, baseCityID);
                            accessedKeysPerActor.Add(grainID, new HashSet<ISnapperKey> { id });
                        }
                        else if (actorType.Contains("ShipmentActor"))
                        {
                            (var sellerID, var baseCityID) = Helper.ConvertGuidToTwoInts(grainID.id);
                            var id = new SellerID(sellerID, baseCityID);
                            var nextPackageIDKey = new GeneralStringKey("nextPackageID");
                            accessedKeysPerActor.Add(grainID, new HashSet<ISnapperKey> { nextPackageIDKey, id });
                        }
                        else if (actorType.Contains("StockActor"))
                        {
                            (var sellerID, var cityID) = Helper.ConvertGuidToTwoInts(grainID.id);
                            var baseCityID = baseCityPerSeller[sellerID];
                            var id = new SellerID(sellerID, baseCityID);
                            accessedKeysPerActor.Add(grainID, new HashSet<ISnapperKey> { id });
                        }
                        tasks.Add(pm.SubmitTransaction(grainID, startFunc, new List<GrainID> { grainID }, accessedKeysPerActor));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        if (actorType.Contains("CartActor"))
                        {
                            var cartActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalCartActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(cartActor.Init(input));
                        }
                        else if (actorType.Contains("CustomerActor"))
                        {
                            var customerActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalCustomerActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(customerActor.Init(input));
                        }
                        else if (actorType.Contains("OrderActor"))
                        {
                            var orderActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalOrderActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(orderActor.Init(input));
                        }
                        else if (actorType.Contains("PaymentActor"))
                        {
                            var paymentActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalPaymentActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(paymentActor.Init(input));
                        }
                        else if (actorType.Contains("ProductActor"))
                        {
                            var productActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalProductActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(productActor.Init(input));
                        }
                        else if (actorType.Contains("SellerActor"))
                        {
                            var sellerActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalSellerActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(sellerActor.Init(input));
                        }
                        else if (actorType.Contains("ShipmentActor"))
                        {
                            var shipmentActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalShipmentActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(shipmentActor.Init(input));
                        }
                        else if (actorType.Contains("StockActor"))
                        {
                            var stockActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalStockActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(stockActor.Init(input));
                        }
                        break;
                    case ImplementationType.NONTXN:
                        if (actorType.Contains("CartActor"))
                        {
                            var cartActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleCartActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(cartActor.Init(input));
                        }
                        else if (actorType.Contains("CustomerActor"))
                        {
                            var customerActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleCustomerActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(customerActor.Init(input));
                        }
                        else if (actorType.Contains("OrderActor"))
                        {
                            var orderActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleOrderActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(orderActor.Init(input));
                        }
                        else if (actorType.Contains("PaymentActor"))
                        {
                            var paymentActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimplePaymentActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(paymentActor.Init(input));
                        }
                        else if (actorType.Contains("ProductActor"))
                        {
                            var productActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleProductActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(productActor.Init(input));
                        }
                        else if (actorType.Contains("SellerActor"))
                        {
                            var sellerActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleSellerActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(sellerActor.Init(input));
                        }
                        else if (actorType.Contains("ShipmentActor"))
                        {
                            var shipmentActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleShipmentActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(shipmentActor.Init(input));
                        }
                        else if (actorType.Contains("StockActor"))
                        {
                            var stockActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleStockActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            tasks.Add(stockActor.Init(input));
                        }
                        break;
                    default:
                        throw new Exception($"The implementationType {implementationType} is not supported. ");
                }

                if (tasks.Count % batchSize == 0)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            await Task.WhenAll(tasks);
            tasks.Clear();

            var strs = actorType.Split(".");
            Console.WriteLine($"Init {item.Value.Count} {strs.Last()} actors. ");
        }

        // put some products to each product actor
        var numReRun = 0;
        var productActors = grainsInSilo[nameMap["ProductActor"]];
        foreach (var grainID in productActors)
        {
            var succeed = false;
            while (!succeed)
            {
                (var sellerID, var baseCityID) = Helper.ConvertGuidToTwoInts(grainID.id);

                var products = new Dictionary<ProductID, ProductInfo>();
                for (var id = 0; id < envSetting.numProductPerSeller; id++)
                {
                    var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                    var productInfo = new ProductInfo("name", rnd.Next(0, envSetting.maxPricePerProduct));
                    products.Add(productID, productInfo);
                }

                TransactionResult? res;
                switch (implementationType)
                {
                    case ImplementationType.SNAPPER:
                        var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                        var method = typeof(SnapperTransactionalKeyValueProductActor).GetMethod("AddProducts");
                        Debug.Assert(method != null);
                        res = await pm.SubmitTransaction(grainID, new FunctionCall(method, products));
                        break;
                    case ImplementationType.SNAPPERSIMPLE:
                        var pmID1 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm1 = grainFactory.GetGrain<IGrainPlacementManager>(pmID1, myRegion + "+" + mySilo);
                        var method1 = typeof(SnapperTransactionalSimpleProductActor).GetMethod("AddProducts");
                        Debug.Assert(method1 != null);
                        res = await pm1.SubmitTransaction(grainID, new FunctionCall(method1, products));
                        break;
                    case ImplementationType.SNAPPERFINE:
                        var pmID2 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm2 = grainFactory.GetGrain<IGrainPlacementManager>(pmID2, myRegion + "+" + mySilo);
                        var method2 = typeof(SnapperTransactionalFineProductActor).GetMethod("AddProducts");
                        Debug.Assert(method2 != null);
                        res = await pm2.SubmitTransaction(grainID, new FunctionCall(method2, products));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var productActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalProductActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                        res = await productActor.AddProducts(products);
                        break;
                    case ImplementationType.NONTXN:
                        var productActorEventual = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleProductActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                        res = await productActorEventual.AddProducts(products);
                        break;
                    case ImplementationType.NONTXNKV:
                        var pmID3 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm3 = grainFactory.GetGrain<IGrainPlacementManager>(pmID3, myRegion + "+" + mySilo);
                        var method3 = typeof(NonTransactionalKeyValueProductActor).GetMethod("AddProducts");
                        Debug.Assert(method3 != null);
                        res = await pm3.SubmitNonTransactionalRequest(grainID, new FunctionCall(method3, products));
                        break;
                    default:
                        throw new Exception($"The implementationType {implementationType} is not supported. ");
                }

                Debug.Assert(res != null);
                if (!res.hasException()) succeed = true;
                else numReRun++;
            }
        }
        Console.WriteLine($"Add {envSetting.numProductPerSeller} products to {productActors.Count} product actors, # re-run = {numReRun}. ");

        // put stock info to each stock actor
        numReRun = 0;
        var stockActors = grainsInSilo[nameMap["StockActor"]];
        foreach (var grainID in stockActors)
        {
            (var sellerID, var cityID) = Helper.ConvertGuidToTwoInts(grainID.id);
            var baseCityID = baseCityPerSeller[sellerID];
            var productActorGuid = IdMapping.GetProductActorGuid(sellerID, baseCityID);
            var productActorID = new GrainID(productActorGuid, nameMap["ProductActor"]);

            var productIDs = new HashSet<int>();
            while (productIDs.Count < envSetting.numProductPerStockCity) productIDs.Add(productDistribution.Sample());

            var stockInfo = new StockInfo(rnd.Next(0, envSetting.maxQuantityInStockPerProduct), "address");

            var succeed = false;
            TransactionResult? res;
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                    tasks.Clear();
                    foreach (var id in productIDs)
                    {
                        var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                        var stock = (productID, stockInfo);
                        var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                        var method = typeof(SnapperTransactionalKeyValueStockActor).GetMethod("AddStock");
                        Debug.Assert(method != null);
                        tasks.Add(pm.SubmitTransaction(grainID, new FunctionCall(method, stock), new List<GrainID> { grainID, productActorID }));
                    }
                    await Task.WhenAll(tasks);
                    break;
                case ImplementationType.SNAPPERSIMPLE:
                    tasks.Clear();
                    foreach (var id in productIDs)
                    {
                        var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                        var stock = (productID, stockInfo);
                        var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                        var method = typeof(SnapperTransactionalSimpleStockActor).GetMethod("AddStock");
                        Debug.Assert(method != null);
                        tasks.Add(pm.SubmitTransaction(grainID, new FunctionCall(method, stock), new List<GrainID> { grainID, productActorID }));
                    }
                    await Task.WhenAll(tasks);
                    break;
                case ImplementationType.SNAPPERFINE:
                    tasks.Clear();
                    foreach (var id in productIDs)
                    {
                        var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                        var stock = (productID, stockInfo);
                        var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                        var method = typeof(SnapperTransactionalFineStockActor).GetMethod("AddStock");
                        Debug.Assert(method != null);
                        var accessedKeysPerActor = new Dictionary<GrainID, HashSet<ISnapperKey>>
                        {
                            { grainID, new HashSet<ISnapperKey>{ productID } },
                            { productActorID, new HashSet<ISnapperKey>{ productID } }
                        };
                        tasks.Add(pm.SubmitTransaction(grainID, new FunctionCall(method, stock), new List<GrainID> { grainID, productActorID }, accessedKeysPerActor));
                    }
                    await Task.WhenAll(tasks);
                    break;
                case ImplementationType.ORLEANSTXN:
                    foreach (var id in productIDs)
                    {
                        var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                        var stock = (productID, stockInfo);

                        while (!succeed)
                        {
                            var stockActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalStockActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await stockActor.AddStock(stock);
                            Debug.Assert(res != null);
                            if (!res.hasException())
                            {
                                Debug.Assert(res.resultObj != null);
                                succeed = (bool)res.resultObj;
                            }
                        }
                    }
                    break;
                case ImplementationType.NONTXN:
                    foreach (var id in productIDs)
                    {
                        var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                        var stock = (productID, stockInfo);
                        var stockActorEventual = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleStockActor>(snapperClusterCache, grainFactory, myRegion, grainID);

                        while (!succeed)
                        {
                            res = await stockActorEventual.AddStock(stock);
                            Debug.Assert(res != null);
                            if (!res.hasException())
                            {
                                Debug.Assert(res.resultObj != null);
                                succeed = (bool)res.resultObj;
                            }
                        }
                    }
                    break;
                case ImplementationType.NONTXNKV:
                    foreach (var id in productIDs)
                    {
                        var productID = new ProductID(new SellerID(sellerID, baseCityID), id);
                        var stock = (productID, stockInfo);
                        var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                        var method = typeof(NonTransactionalKeyValueStockActor).GetMethod("AddStock");
                        Debug.Assert(method != null);

                        while (!succeed)
                        {
                            res = await pm.SubmitNonTransactionalRequest(grainID, new FunctionCall(method, stock));
                            Debug.Assert(res != null);
                            if (!res.hasException())
                            {
                                Debug.Assert(res.resultObj != null);
                                succeed = (bool)res.resultObj;
                            }
                        }
                    }
                    break;
                default:
                    throw new Exception($"The implementationType {implementationType} is not supported. ");
            }
        }
        Console.WriteLine($"Add {envSetting.numProductPerStockCity} product stock info to {stockActors.Count} stock actors, # re-run = {numReRun}. ");

        /*
        // execute some transactions before doing experiemnts
        numReRun = 0;
        var cartActors = grainsInSilo[nameMap["CartActor"]];
        foreach (var grainID in cartActors)
        {
            (var customerID, var baseCityID) = Helper.ConvertGuidToTwoInts(grainID.id);

            var items = new HashSet<ProductID>();
            while (items.Count < envSetting.initialNumProductPerCart)
            {
                var sellerID = sellerDistribution.Sample();
                var productID = productDistribution.Sample();
                items.Add(new ProductID(new SellerID(sellerID, baseCityPerSeller[sellerID]), productID));
            }

            foreach (var productID in items)
            {
                var succeed = false;
                while (!succeed)
                {
                    TransactionResult? res;
                    switch (implementationType)
                    {
                        case ImplementationType.SNAPPER:
                            var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                            var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);
                            var method = typeof(SnapperTransactionalKeyValueCartActor).GetMethod("AddItemToCart");
                            Debug.Assert(method != null);
                            res = await pm.SubmitTransaction(grainID, new FunctionCall(method, productID));
                            break;
                        case ImplementationType.SNAPPERSIMPLE:
                            var pmID1 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                            var pm1 = grainFactory.GetGrain<IGrainPlacementManager>(pmID1, myRegion + "+" + mySilo);
                            var method1 = typeof(SnapperTransactionalSimpleCartActor).GetMethod("AddItemToCart");
                            Debug.Assert(method1 != null);
                            res = await pm1.SubmitTransaction(grainID, new FunctionCall(method1, productID));
                            break;
                        case ImplementationType.SNAPPERFINE:
                            var pmID2 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                            var pm2 = grainFactory.GetGrain<IGrainPlacementManager>(pmID2, myRegion + "+" + mySilo);
                            var method2 = typeof(SnapperTransactionalFineCartActor).GetMethod("AddItemToCart");
                            Debug.Assert(method2 != null);
                            res = await pm2.SubmitTransaction(grainID, new FunctionCall(method2, productID));
                            break;
                        case ImplementationType.ORLEANSTXN:
                            var cartActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalCartActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await cartActor.AddItemToCart(productID);
                            break;
                        case ImplementationType.NONTXN:
                            var cartActorEventual = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleCartActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await cartActorEventual.AddItemToCart(productID);
                            break;
                        case ImplementationType.NONTXNKV:
                            var pmID3 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                            var pm3 = grainFactory.GetGrain<IGrainPlacementManager>(pmID3, myRegion + "+" + mySilo);
                            var method3 = typeof(NonTransactionalKeyValueCartActor).GetMethod("AddItemToCart");
                            Debug.Assert(method3 != null);
                            res = await pm3.SubmitNonTransactionalRequest(grainID, new FunctionCall(method3, productID));
                            break;
                        default:
                            throw new Exception($"The implementationType {implementationType} is not supported. ");
                    }

                    
                    if (!res.hasException()) succeed = true;
                    else numReRun++;
                }
            }
        }
        Console.WriteLine($"Add {envSetting.initialNumProductPerCart} items to {cartActors.Count} cart actors, # re-run = {numReRun}. ");
        */
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory)
    {
        var result = new Dictionary<GrainID, (byte[], byte[], byte[], byte[])>();
        var myPMListPerSilo = snapperClusterCache.GetPMList(myRegion);

        MethodInfo? method = null;
        FunctionCall? startFunc = null;
        TransactionResult res;
        var grainsInSilo = grains[myRegion][mySilo];
        foreach (var item in grainsInSilo)
        {
            foreach (var grainID in item.Value)
            {
                switch (implementationType)
                {
                    case ImplementationType.SNAPPER:
                    case ImplementationType.SNAPPERFINE:
                        var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm = grainFactory.GetGrain<IGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

                        Debug.Assert(grainID.className != null);
                        startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());
                        res = await pm.SubmitTransaction(grainID, startFunc);
                        break;
                    case ImplementationType.NONTXNKV:
                        var pmID2 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm2 = grainFactory.GetGrain<IGrainPlacementManager>(pmID2, myRegion + "+" + mySilo);

                        Debug.Assert(grainID.className != null);
                        startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());
                        res = await pm2.SubmitNonTransactionalRequest(grainID, startFunc);
                        break;
                    case ImplementationType.SNAPPERSIMPLE:
                        var pmID1 = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                        var pm1 = grainFactory.GetGrain<IGrainPlacementManager>(pmID1, myRegion + "+" + mySilo);

                        Debug.Assert(grainID.className != null);
                        if (grainID.className.Contains("CartActor"))
                        {
                            method = typeof(SnapperTransactionalSimpleCartActor).GetMethod("ReadSimpleState");
                            Debug.Assert(method != null);
                            startFunc = new FunctionCall(method);
                            res = await pm1.SubmitTransaction(grainID, startFunc);
                        }
                        else if (grainID.className.Contains("OrderActor"))
                        {
                            method = typeof(SnapperTransactionalSimpleOrderActor).GetMethod("ReadSimpleState");
                            Debug.Assert(method != null);
                            startFunc = new FunctionCall(method);
                            res = await pm1.SubmitTransaction(grainID, startFunc);
                        }
                        else if (grainID.className.Contains("ProductActor"))
                        {
                            method = typeof(SnapperTransactionalSimpleProductActor).GetMethod("ReadSimpleState");
                            Debug.Assert(method != null);
                            startFunc = new FunctionCall(method);
                            res = await pm1.SubmitTransaction(grainID, startFunc);
                        }
                        else if (grainID.className.Contains("ShipmentActor"))
                        {
                            method = typeof(SnapperTransactionalSimpleShipmentActor).GetMethod("ReadSimpleState");
                            Debug.Assert(method != null);
                            startFunc = new FunctionCall(method);
                            res = await pm1.SubmitTransaction(grainID, startFunc);
                        }
                        else if (grainID.className.Contains("StockActor"))
                        {
                            method = typeof(SnapperTransactionalSimpleStockActor).GetMethod("ReadSimpleState");
                            Debug.Assert(method != null);
                            startFunc = new FunctionCall(method);
                            res = await pm1.SubmitTransaction(grainID, startFunc);
                        }
                        else continue;
                        break;
                    case ImplementationType.ORLEANSTXN:
                        if (grainID.className.Contains("CartActor"))
                        {
                            var cartActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalCartActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await cartActor.ReadState();
                        }
                        else if (grainID.className.Contains("OrderActor"))
                        {
                            var orderActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalOrderActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await orderActor.ReadState();
                        }
                        else if (grainID.className.Contains("ProductActor"))
                        {
                            var productActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalProductActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await productActor.ReadState();
                        }
                        else if (grainID.className.Contains("ShipmentActor"))
                        {
                            var shipmentActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalShipmentActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await shipmentActor.ReadState();
                        }
                        else if (grainID.className.Contains("StockActor"))
                        {
                            var stockActor = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalStockActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await stockActor.ReadState();
                        }
                        else continue;
                        break;
                    case ImplementationType.NONTXN:
                        if (grainID.className.Contains("CartActor"))
                        {
                            var cartActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleCartActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await cartActor.ReadState();
                        }
                        else if (grainID.className.Contains("OrderActor"))
                        {
                            var orderActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleOrderActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await orderActor.ReadState();
                        }
                        else if (grainID.className.Contains("ProductActor"))
                        {
                            var productActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleProductActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await productActor.ReadState();
                        }
                        else if (grainID.className.Contains("ShipmentActor"))
                        {
                            var shipmentActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleShipmentActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await shipmentActor.ReadState();
                        }
                        else if (grainID.className.Contains("StockActor"))
                        {
                            var stockActor = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleStockActor>(snapperClusterCache, grainFactory, myRegion, grainID);
                            res = await stockActor.ReadState();
                        }
                        else continue;
                        break;
                    default:
                        throw new Exception($"The implementationType {implementationType} is not supported. ");
                }

                Debug.Assert(res.resultObj != null);
                result.Add(grainID, ((byte[], byte[], byte[], byte[]))res.resultObj);
            }
            var strs = item.Key.Split(".");
            Console.WriteLine($"Get state of {item.Value.Count} {strs.Last()}. ");
        }

        return result;
    }

    public async Task<AggregatedExperimentData> CheckGC(string myRegion, string mySilo, ImplementationType implementationType, IGrainFactory grainFactory)
    {
        var data = new ExperimentData();
        var tasks = new List<Task<AggregatedExperimentData>>();
        var grainsInSilo = grains[myRegion][mySilo];
        foreach (var item in grainsInSilo)
        {
            foreach (var id in item.Value)
            {
                switch (implementationType)
                {
                    case ImplementationType.SNAPPERSIMPLE:
                    case ImplementationType.SNAPPER:
                    case ImplementationType.SNAPPERFINE:
                        var actor = grainFactory.GetGrain<ITransactionExecutionGrain>(id.id, myRegion + "+" + mySilo, id.className);
                        tasks.Add(actor.CheckGC());
                        break;
                    case ImplementationType.NONTXNKV:
                        var actor1 = grainFactory.GetGrain<INonTransactionalKeyValueGrain>(id.id, myRegion + "+" + mySilo, id.className);
                        tasks.Add(actor1.CheckGC());
                        break;
                }
            }
            await Task.WhenAll(tasks);

            foreach (var t in tasks) data.Set(t.Result);

            tasks.Clear();
        }

        return data.AggregateAndClear();
    }

    public async Task InitAllReplicaGrains(string myRegion, string mySilo, IGrainFactory grainFactory)
    {
        if (!replicaGrains.ContainsKey(myRegion)) replicaGrains.Add(myRegion, new Dictionary<string, Dictionary<string, List<GrainID>>>());
        if (!replicaGrains[myRegion].ContainsKey(mySilo)) replicaGrains[myRegion].Add(mySilo, new Dictionary<string, List<GrainID>>());

        var replicaGrainsInSilo = new Dictionary<string, List<GrainID>>();
        foreach (var item in replicaGrainIDToPartitionID)
        {
            var actorID = item.Key;
            var partitionID = item.Value;
            var siloID = partitionIDToReplicaInfo[myRegion][partitionID];
            if (siloID != mySilo) continue;
            else
            {
                Debug.Assert(actorID.className != null);
                if (!replicaGrainsInSilo.ContainsKey(actorID.className)) replicaGrainsInSilo.Add(actorID.className, new List<GrainID>());
                replicaGrainsInSilo[actorID.className].Add(actorID);
            } 
        }
        replicaGrains[myRegion][mySilo] = replicaGrainsInSilo;

        var batchSize = 100;
        var tasks = new List<Task>();
        foreach (var item in replicaGrainsInSilo)
        {
            foreach (var id in item.Value)
            {
                var grain = grainFactory.GetGrain<ITransactionReplicationGrain>(id.id, myRegion + "+" + mySilo, id.className);
                tasks.Add(grain.Init());

                if (tasks.Count % batchSize == 0)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            Console.WriteLine($"Init {item.Value.Count} {item.Key}. ");
        }
    }

    public async Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllReplicaGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperReplicaCache snapperReplicaCache, IGrainFactory grainFactory)
    {
        var myPMListPerSilo = snapperReplicaCache.GetPMList(myRegion);
        var replicaGrainsInSilo = replicaGrains[myRegion][mySilo];

        var result = new Dictionary<GrainID, (byte[], byte[], byte[], byte[])>();
        foreach (var item in replicaGrainsInSilo)
        {
            if (item.Key.Contains("SellerActor")) continue;

            foreach (var id in item.Value)
            {
                var pmID = myPMListPerSilo[mySilo][new Random().Next(0, myPMListPerSilo[mySilo].Count)];
                var pm = grainFactory.GetGrain<IReplicaGrainPlacementManager>(pmID, myRegion + "+" + mySilo);

                Debug.Assert(id.className != null);
                var startFunc = new FunctionCall(SnapperInternalFunction.ReadState.ToString());

                var res = await pm.SubmitTransaction(id, startFunc);
                Debug.Assert(res.resultObj != null);
                result.Add(id, ((byte[], byte[], byte[], byte[]))res.resultObj);
            }
            Console.WriteLine($"Init {item.Value.Count} {item.Key}. ");
        }

        return result;
    }

    public async Task CheckGCForReplica(string myRegion, string mySilo, IGrainFactory grainFactory)
    {
        var replicaGrainsInSilo = replicaGrains[myRegion][mySilo];

        var tasks = new List<Task>();
        foreach (var item in replicaGrainsInSilo)
        {
            foreach (var id in item.Value)
            {
                var actor = grainFactory.GetGrain<ITransactionReplicationGrain>(id.id, myRegion + "+" + mySilo, id.className);
                tasks.Add(actor.CheckGC());
            }
            await Task.WhenAll(tasks);
        }
    }
}
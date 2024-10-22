using Concurrency.Common;
using Experiment.Common;
using MarketPlace.Grains.NonTransactionalKeyValueGrains;
using MarketPlace.Grains.OrleansTransactionalGrains;
using MarketPlace.Grains.NonTransactionalSimpleGrains;
using MarketPlace.Grains.SnapperTransactionalFineGrains;
using MarketPlace.Grains.SnapperTransactionalKeyValueGrains;
using MarketPlace.Grains.SnapperTransactionalSimpleGrains;
using System.Diagnostics;
using System.Reflection;
using Utilities;
using MarketPlace.Grains.SnapperReplicatedKeyValueGrains;

namespace MarketPlace.Workload;

public class GrainNameHelper : IGrainNameHelper
{
    static Dictionary<ImplementationType, Dictionary<string, string>> nameMapping = new Dictionary<ImplementationType, Dictionary<string, string>>
    {
        { ImplementationType.SNAPPER, new Dictionary<string, string>
            {
                { typeof(SnapperReplicatedKeyValueStockActor).FullName,    typeof(SnapperTransactionalKeyValueStockActor).FullName },
                { typeof(SnapperReplicatedKeyValueShipmentActor).FullName, typeof(SnapperTransactionalKeyValueShipmentActor).FullName }
            } 
        }
    };

    static Dictionary<ImplementationType, Dictionary<string, Type>> snapperTypeMapping = new Dictionary<ImplementationType, Dictionary<string, Type>>
    {
        { ImplementationType.NONTXN, new Dictionary<string, Type>
            {
                { typeof(NonTransactionalSimpleCustomerActor).FullName, typeof(NonTransactionalSimpleCustomerActor) },
                { typeof(NonTransactionalSimpleCartActor).FullName,     typeof(NonTransactionalSimpleCartActor) },
                { typeof(NonTransactionalSimplePaymentActor).FullName,  typeof(NonTransactionalSimplePaymentActor) },
                { typeof(NonTransactionalSimpleOrderActor).FullName,    typeof(NonTransactionalSimpleOrderActor) },

                { typeof(NonTransactionalSimpleProductActor).FullName,  typeof(NonTransactionalSimpleProductActor) },
                { typeof(NonTransactionalSimpleStockActor).FullName,    typeof(NonTransactionalSimpleStockActor) },
                { typeof(NonTransactionalSimpleShipmentActor).FullName, typeof(NonTransactionalSimpleShipmentActor) },
                { typeof(NonTransactionalSimpleSellerActor).FullName,   typeof(NonTransactionalSimpleSellerActor) }
            } 
        },
        { ImplementationType.ORLEANSTXN, new Dictionary<string, Type>
            {
                { typeof(OrleansTransactionalCustomerActor).FullName, typeof(OrleansTransactionalCustomerActor) },
                { typeof(OrleansTransactionalCartActor).FullName,     typeof(OrleansTransactionalCartActor) },
                { typeof(OrleansTransactionalPaymentActor).FullName,  typeof(OrleansTransactionalPaymentActor) },
                { typeof(OrleansTransactionalOrderActor).FullName,    typeof(OrleansTransactionalOrderActor) },

                { typeof(OrleansTransactionalProductActor).FullName,  typeof(OrleansTransactionalProductActor) },
                { typeof(OrleansTransactionalStockActor).FullName,    typeof(OrleansTransactionalStockActor) },
                { typeof(OrleansTransactionalShipmentActor).FullName, typeof(OrleansTransactionalShipmentActor) },
                { typeof(OrleansTransactionalSellerActor).FullName,   typeof(OrleansTransactionalSellerActor) }
            }
        },
        { ImplementationType.SNAPPERSIMPLE, new Dictionary<string, Type>
            {
                { typeof(SnapperTransactionalSimpleCustomerActor).FullName, typeof(SnapperTransactionalSimpleCustomerActor) },
                { typeof(SnapperTransactionalSimpleCartActor).FullName,     typeof(SnapperTransactionalSimpleCartActor) },
                { typeof(SnapperTransactionalSimplePaymentActor).FullName,  typeof(SnapperTransactionalSimplePaymentActor) },
                { typeof(SnapperTransactionalSimpleOrderActor).FullName,    typeof(SnapperTransactionalSimpleOrderActor) },

                { typeof(SnapperTransactionalSimpleProductActor).FullName,  typeof(SnapperTransactionalSimpleProductActor) },
                { typeof(SnapperTransactionalSimpleStockActor).FullName,    typeof(SnapperTransactionalSimpleStockActor) },
                { typeof(SnapperTransactionalSimpleShipmentActor).FullName, typeof(SnapperTransactionalSimpleShipmentActor) },
                { typeof(SnapperTransactionalSimpleSellerActor).FullName,   typeof(SnapperTransactionalSimpleSellerActor) }
            }
        },
        { ImplementationType.SNAPPER, new Dictionary<string, Type>
            {
                { typeof(SnapperTransactionalKeyValueCustomerActor).FullName, typeof(SnapperTransactionalKeyValueCustomerActor) },
                { typeof(SnapperTransactionalKeyValueCartActor).FullName,     typeof(SnapperTransactionalKeyValueCartActor) },
                { typeof(SnapperTransactionalKeyValuePaymentActor).FullName,  typeof(SnapperTransactionalKeyValuePaymentActor) },
                { typeof(SnapperTransactionalKeyValueOrderActor).FullName,    typeof(SnapperTransactionalKeyValueOrderActor) },

                { typeof(SnapperTransactionalKeyValueProductActor).FullName,  typeof(SnapperTransactionalKeyValueProductActor) },
                { typeof(SnapperTransactionalKeyValueStockActor).FullName,    typeof(SnapperTransactionalKeyValueStockActor) },
                { typeof(SnapperTransactionalKeyValueShipmentActor).FullName, typeof(SnapperTransactionalKeyValueShipmentActor) },
                { typeof(SnapperTransactionalKeyValueSellerActor).FullName, typeof(SnapperTransactionalKeyValueSellerActor) },

                { typeof(SnapperReplicatedKeyValueSellerActor).FullName,      typeof(SnapperReplicatedKeyValueSellerActor) },
                { typeof(SnapperReplicatedKeyValueStockActor).FullName,       typeof(SnapperReplicatedKeyValueStockActor) },
                { typeof(SnapperReplicatedKeyValueShipmentActor).FullName,    typeof(SnapperReplicatedKeyValueShipmentActor) }
            }
        },
        { ImplementationType.SNAPPERFINE, new Dictionary<string, Type>
            {
                { typeof(SnapperTransactionalFineCustomerActor).FullName, typeof(SnapperTransactionalFineCustomerActor) },
                { typeof(SnapperTransactionalFineCartActor).FullName,     typeof(SnapperTransactionalFineCartActor) },
                { typeof(SnapperTransactionalFinePaymentActor).FullName,  typeof(SnapperTransactionalFinePaymentActor) },
                { typeof(SnapperTransactionalFineOrderActor).FullName,    typeof(SnapperTransactionalFineOrderActor) },

                { typeof(SnapperTransactionalFineProductActor).FullName,  typeof(SnapperTransactionalFineProductActor) },
                { typeof(SnapperTransactionalFineStockActor).FullName,    typeof(SnapperTransactionalFineStockActor) },
                { typeof(SnapperTransactionalFineShipmentActor).FullName, typeof(SnapperTransactionalFineShipmentActor) },
                { typeof(SnapperTransactionalFineSellerActor).FullName,   typeof(SnapperTransactionalFineSellerActor) }
            }
        }
    };

    static Dictionary<ImplementationType, Dictionary<string, string>> nameMap = new Dictionary<ImplementationType, Dictionary<string, string>>
    {
        { ImplementationType.NONTXN, new Dictionary<string, string>
            {
                { "CartActor", typeof(NonTransactionalSimpleCartActor).FullName },
                { "CustomerActor", typeof(NonTransactionalSimpleCustomerActor).FullName },
                { "OrderActor", typeof(NonTransactionalSimpleOrderActor).FullName },
                { "PaymentActor", typeof(NonTransactionalSimplePaymentActor).FullName },
                { "ProductActor", typeof(NonTransactionalSimpleProductActor).FullName },
                { "ShipmentActor", typeof(NonTransactionalSimpleShipmentActor).FullName },
                { "StockActor", typeof(NonTransactionalSimpleStockActor).FullName },
                { "SellerActor", typeof(NonTransactionalSimpleSellerActor).FullName}
            }
        },
        { ImplementationType.ORLEANSTXN, new Dictionary<string, string>
            {
                { "CartActor", typeof(OrleansTransactionalCartActor).FullName },
                { "CustomerActor", typeof(OrleansTransactionalCustomerActor).FullName },
                { "OrderActor", typeof(OrleansTransactionalOrderActor).FullName },
                { "PaymentActor", typeof(OrleansTransactionalPaymentActor).FullName },
                { "ProductActor", typeof(OrleansTransactionalProductActor).FullName },
                { "ShipmentActor", typeof(OrleansTransactionalShipmentActor).FullName },
                { "StockActor", typeof(OrleansTransactionalStockActor).FullName },
                { "SellerActor", typeof(OrleansTransactionalSellerActor).FullName }
            }
        },
        { ImplementationType.SNAPPERSIMPLE, new Dictionary<string, string>
            {
                { "CartActor", typeof(SnapperTransactionalSimpleCartActor).FullName },
                { "CustomerActor", typeof(SnapperTransactionalSimpleCustomerActor).FullName },
                { "OrderActor", typeof(SnapperTransactionalSimpleOrderActor).FullName },
                { "PaymentActor", typeof(SnapperTransactionalSimplePaymentActor).FullName },
                { "ProductActor", typeof(SnapperTransactionalSimpleProductActor).FullName },
                { "ShipmentActor", typeof(SnapperTransactionalSimpleShipmentActor).FullName },
                { "StockActor", typeof(SnapperTransactionalSimpleStockActor).FullName },
                { "SellerActor", typeof(SnapperTransactionalSimpleSellerActor).FullName }
            }
        },
        { ImplementationType.SNAPPER, new Dictionary<string, string>
            {
                { "CartActor", typeof(SnapperTransactionalKeyValueCartActor).FullName },
                { "CustomerActor", typeof(SnapperTransactionalKeyValueCustomerActor).FullName },
                { "OrderActor", typeof(SnapperTransactionalKeyValueOrderActor).FullName },
                { "PaymentActor", typeof(SnapperTransactionalKeyValuePaymentActor).FullName },
                { "ProductActor", typeof(SnapperTransactionalKeyValueProductActor).FullName },
                { "ShipmentActor", typeof(SnapperTransactionalKeyValueShipmentActor).FullName },
                { "StockActor", typeof(SnapperTransactionalKeyValueStockActor).FullName },
                { "SellerActor", typeof(SnapperTransactionalKeyValueSellerActor).FullName },

                { "ReplicatedSellerActor", typeof(SnapperReplicatedKeyValueSellerActor).FullName },
                { "ReplicatedStockActor", typeof(SnapperReplicatedKeyValueStockActor).FullName },
                { "ReplicatedShipmentActor", typeof(SnapperReplicatedKeyValueShipmentActor).FullName }
            } 
        },
        { ImplementationType.SNAPPERFINE, new Dictionary<string, string>
            {
                { "CartActor", typeof(SnapperTransactionalFineCartActor).FullName },
                { "CustomerActor", typeof(SnapperTransactionalFineCustomerActor).FullName },
                { "OrderActor", typeof(SnapperTransactionalFineOrderActor).FullName },
                { "PaymentActor", typeof(SnapperTransactionalFinePaymentActor).FullName },
                { "ProductActor", typeof(SnapperTransactionalFineProductActor).FullName },
                { "ShipmentActor", typeof(SnapperTransactionalFineShipmentActor).FullName },
                { "StockActor", typeof(SnapperTransactionalFineStockActor).FullName },
                { "SellerActor", typeof(SnapperTransactionalFineSellerActor).FullName }
            }
        }
    };

    public static MethodInfo GetMethod(ImplementationType implementationType, string grainClassName, string funcName)
    {
        var snapperMethod = snapperTypeMapping[implementationType][grainClassName].GetMethod(funcName);
        if (snapperMethod == null) Console.WriteLine($"Fail to find method {funcName} on class {grainClassName}");
        Debug.Assert(snapperMethod != null);
        return snapperMethod;
    }

    public GrainID GetMasterGrainID(ImplementationType implementationType, GrainID replicaGrainID)
    {
        var guid = replicaGrainID.id;
        var name = nameMapping[implementationType][replicaGrainID.className];
        return new GrainID(guid, name);
    }

    public Dictionary<string, string> GetNameMap(ImplementationType implementationType) => nameMap[implementationType];
}
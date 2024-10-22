using Concurrency.Common;
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;

[GenerateSerializer]
[MessagePackObject]
public class OrderActorState : AbstractValue<OrderActorState>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public GeneralLongValue nextOrderID;

    [Key(1)]
    [Id(1)]
    public Dictionary<PackageID, PackageInfo> packageInfo;

    [Key(2)]
    [Id(2)]
    public Dictionary<PackageID, GrainID> dependencies;

    [Key(3)]
    [Id(3)]
    public Dictionary<OrderID, OrderInfo> orderInfo;

    public OrderActorState()
    { 
        nextOrderID = new GeneralLongValue();
        packageInfo = new Dictionary<PackageID, PackageInfo>();
        dependencies = new Dictionary<PackageID, GrainID>();
        orderInfo = new Dictionary<OrderID, OrderInfo>();
    }

    public OrderActorState(GeneralLongValue nextOrderID, Dictionary<PackageID, PackageInfo> packageInfo, Dictionary<PackageID, GrainID> dependencies, Dictionary<OrderID, OrderInfo> orderInfo)
    { 
        this.nextOrderID = nextOrderID;
        this.packageInfo = packageInfo;
        this.dependencies = dependencies;
        this.orderInfo = orderInfo;
    }

    public object Clone() => new OrderActorState(new GeneralLongValue(nextOrderID.value), Helper.CloneDictionary(packageInfo), Helper.CloneDictionary(dependencies), Helper.CloneDictionary(orderInfo));
   
    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as OrderActorState;
            if (data == null) return false;

            if (!data.nextOrderID.Equals(nextOrderID)) return false;
            if (!Helper.CheckDictionaryEqualty(data.packageInfo, packageInfo)) return false;
            if (!Helper.CheckDictionaryEqualty(data.dependencies, dependencies)) return false;
            if (!Helper.CheckDictionaryEqualty(data.orderInfo, orderInfo)) return false;
            return true;
        }
        catch { return false; }
    }

    public string Print() => "OrderActorState";
}
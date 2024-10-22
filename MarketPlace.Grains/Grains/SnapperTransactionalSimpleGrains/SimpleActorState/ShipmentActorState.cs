using Concurrency.Common;
using Concurrency.Common.State;
using MarketPlace.Grains.KeyState;
using MarketPlace.Grains.ValueState;
using MessagePack;
using Utilities;

namespace MarketPlace.Grains.SnapperTransactionalSimpleGrains.SimpleActorState;

[GenerateSerializer]
[MessagePackObject]
public class ShipmentActorState : AbstractValue<ShipmentActorState>, ISnapperValue, IEquatable<ShipmentActorState>
{
    [Key(0)]
    [Id(0)]
    public GeneralLongValue nextPackageID;

    [Key(1)]
    [Id(1)]
    public Dictionary<PackageID, PackageInfo> packages;

    [Key(2)]
    [Id(2)]
    public Dictionary<PackageID, GrainID> dependencies;

    // keep record of the last created package, whenever a new pacakge is created, the corresponding seller dashboard needs to be updated as well
    [Key(3)]
    [Id(3)]
    public Dictionary<SellerID, PackageInfo> lastCreatedPackagePerSeller;

    [Key(4)]
    [Id(4)]
    public Dictionary<SellerID, GrainID> followerSellerActors;

    public ShipmentActorState() 
    {
        nextPackageID = new GeneralLongValue();
        packages = new Dictionary<PackageID, PackageInfo>();
        dependencies = new Dictionary<PackageID, GrainID>();
        lastCreatedPackagePerSeller = new Dictionary<SellerID, PackageInfo>();
        followerSellerActors = new Dictionary<SellerID, GrainID>();
    }

    public ShipmentActorState(
        GeneralLongValue nextPackageID, 
        Dictionary<PackageID, PackageInfo> packages, 
        Dictionary<PackageID, GrainID> dependencies, 
        Dictionary<SellerID, PackageInfo> lastCreatedPackagePerSeller, 
        Dictionary<SellerID, GrainID> followerSellerActors)
    { 
        this.packages = packages;
        this.nextPackageID = nextPackageID;
        this.dependencies = dependencies;
        this.lastCreatedPackagePerSeller = lastCreatedPackagePerSeller;
        this.followerSellerActors = followerSellerActors;
    }

    public object Clone() => new ShipmentActorState(
        (GeneralLongValue)nextPackageID.Clone(), 
        Helper.CloneDictionary(packages), 
        Helper.CloneDictionary(dependencies), 
        Helper.CloneDictionary(lastCreatedPackagePerSeller),
        Helper.CloneDictionary(followerSellerActors));

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as ShipmentActorState;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(ShipmentActorState? data)
    {
        if (data == null) return false;

        if (!data.nextPackageID.Equals(nextPackageID)) return false;
        if (!Helper.CheckDictionaryEqualty(data.packages, packages)) return false;
        if (!Helper.CheckDictionaryEqualty(data.dependencies, dependencies)) return false;
        if (!Helper.CheckDictionaryEqualty(data.lastCreatedPackagePerSeller, lastCreatedPackagePerSeller)) return false;
        if (!Helper.CheckDictionaryEqualty(data.followerSellerActors, followerSellerActors)) return false;
        return true;
    }

    public string Print() => "ShipmentActorState";
}
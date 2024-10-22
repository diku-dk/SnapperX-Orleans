using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using System.Diagnostics;
using Utilities;

namespace MarketPlace.Grains.NonTransactionalSimpleGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class NonTransactionalSimpleCustomerActor : Grain, INonTransactionalSimpleCustomerActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;

    int customerID;
    int baseCityID;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(NonTransactionalSimpleCustomerActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        return Task.FromResult(new TransactionResult());
    }
}
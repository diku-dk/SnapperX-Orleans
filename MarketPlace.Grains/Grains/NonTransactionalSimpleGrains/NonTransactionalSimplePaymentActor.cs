using Concurrency.Common.ICache;
using Concurrency.Common;
using Concurrency.Implementation.GrainPlacement;
using MarketPlace.Interfaces;
using Orleans.Concurrency;
using Orleans.GrainDirectory;
using Utilities;
using System.Diagnostics;
using MarketPlace.Grains.ValueState;

namespace MarketPlace.Grains.NonTransactionalSimpleGrains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class NonTransactionalSimplePaymentActor : Grain, INonTransactionalSimplePaymentActor
{
    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    int customerID;
    int baseCityID;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], typeof(NonTransactionalSimplePaymentActor).FullName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        (customerID, baseCityID) = Helper.ConvertGuidToTwoInts(myID.grainID.id);
        return Task.FromResult(new TransactionResult());
    }

    public Task<TransactionResult> ProcessPayment(object? obj = null)
    {
        var txnResult = new TransactionResult();

        Debug.Assert(obj != null);
        (var paymentMethod, var money) = ((PaymentMethod, double))obj;

        txnResult.AddGrain(myID);
        txnResult.resultObj = (true, "");
        return Task.FromResult(txnResult);
    }
}
using Concurrency.Common;
using SmallBank.Interfaces;
using Orleans.Concurrency;
using Concurrency.Implementation.GrainPlacement;
using Utilities;
using System.Diagnostics;
using SmallBank.Grains.State;
using Concurrency.Common.ICache;

namespace SmallBank.Grains;

[Reentrant]
[SnapperGrainPlacementStrategy]
public class NonTransactionalSimpleAccountGrain : Grain, INonTransactionalSimpleAccountGrain
{
    readonly string grainName = typeof(NonTransactionalSimpleAccountGrain).FullName;

    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    Dictionary<UserID, AccountInfo> users;

    /// <summary> origin key, follower key, reference type </summary>
    Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>> followersInfo;

    /// <summary> follower key, origin key </summary>
    Dictionary<UserID, UserID> originInfo;

    public NonTransactionalSimpleAccountGrain(ISnapperClusterCache snapperClusterCache) => this.snapperClusterCache = snapperClusterCache;

    public Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], grainName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        Debug.Assert(obj != null);
        var numAccounts = (int)obj;

        users = new Dictionary<UserID, AccountInfo>();
        for (var i = 0; i < numAccounts; i++)
        {
            var userID = SmallBankIdMapping.GetUserID(myID.grainID, i);
            users.Add(userID, new AccountInfo(1000000));
        }

        followersInfo = new Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>();

        originInfo = new Dictionary<UserID, UserID>();

        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);
        return Task.FromResult(txnResult);
    }

    public async Task<TransactionResult> MultiTransfer(object? obj = null)
    {
        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);

        Debug.Assert(obj != null);
        var input = obj as MultiTransferInput;
        Debug.Assert(input != null);

        var writeInfoPerGrain = input.GetWriteInfoPerGrain(grainName);
        var readInfoPerGrain = input.GetReadInfoPerGrain(grainName);

        // do write on the current grain
        if (writeInfoPerGrain.ContainsKey(myID.grainID))
        {
            foreach ((var fromUser, var moneyToWithdraw) in writeInfoPerGrain[myID.grainID])
            {
                if (users.ContainsKey(fromUser)) users[fromUser].balance -= moneyToWithdraw;
            }

            writeInfoPerGrain.Remove(myID.grainID);
        }

        // do read on the current grain
        var total = 0.0;
        if (readInfoPerGrain.ContainsKey(myID.grainID))
        {
            foreach (var user in readInfoPerGrain[myID.grainID])
                total += users.ContainsKey(user) ? users[user].balance : 0;
            
            readInfoPerGrain.Remove(myID.grainID);
        }

        // get the info to forward to other grains
        var depositInputPerGrain = new Dictionary<GrainID, DepositInput>();
        foreach (var item in writeInfoPerGrain)
        {
            var grainID = item.Key;
            var writeInfo = item.Value;
            var readInfo = readInfoPerGrain.ContainsKey(grainID) ? readInfoPerGrain[(grainID)] : new HashSet<UserID>();

            depositInputPerGrain.Add(grainID, new DepositInput(writeInfo, readInfo));
        }

        foreach (var item in readInfoPerGrain)
        {
            var grainID = item.Key;
            var readInfo = item.Value;

            if (writeInfoPerGrain.ContainsKey(grainID)) continue;

            depositInputPerGrain.Add(grainID, new DepositInput(new Dictionary<UserID, int>(), readInfo));
        }

        // forward to other grains
        var method = typeof(SnapperTransactionalFineAccountGrain).GetMethod("Deposit");
        Debug.Assert(method != null);
        var tasks = new List<Task<TransactionResult>>();
        foreach (var info in depositInputPerGrain)
        {
            Debug.Assert(!info.Key.Equals(myID.grainID));

            var grain = GrainReferenceHelper.GetGrainReference<INonTransactionalSimpleAccountGrain>(snapperClusterCache, GrainFactory, myRegionID, info.Key);
            tasks.Add(grain.Deposit(info.Value));
        }
        await Task.WhenAll(tasks);

        var times = new List<double>();
        foreach (var t in tasks)
        {
            txnResult.MergeResult(t.Result);
            Debug.Assert(t.Result.resultObj != null);
            times.Add((double)t.Result.resultObj);
        }

        txnResult.SetResult(times);
        return txnResult;
    }

    public Task<TransactionResult> Deposit(object? obj = null)
    {
        var time = DateTime.UtcNow;
        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);

        Debug.Assert(obj != null);
        var input = obj as DepositInput;
        Debug.Assert(input != null);

        // do write on the current grain
        foreach (var item in input.writeInfo)
        {
            var userID = item.Key;
            var moneyToDeposit = item.Value;

            if (users.ContainsKey(userID)) users[userID].balance += moneyToDeposit;
        }

        // do read on the current grain
        var total = 0.0;
        foreach (var user in input.readInfo)
            total += users.ContainsKey(user) ? users[user].balance : 0;

        txnResult.SetResult((DateTime.UtcNow - time).TotalMilliseconds);
        return Task.FromResult(txnResult);
    }

    public Task<TransactionResult> ReadState()
    {
        var txnResult = new TransactionResult();
        txnResult.resultObj = SmallBankSerializer.SerializeAccountGrainState(users, followersInfo, originInfo);
        return Task.FromResult(txnResult);
    }
}
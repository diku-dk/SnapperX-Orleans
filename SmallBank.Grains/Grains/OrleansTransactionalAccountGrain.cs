using Concurrency.Common;
using Utilities;
using Orleans.Concurrency;
using SmallBank.Interfaces;
using Orleans.Transactions.Abstractions;
using Concurrency.Implementation.GrainPlacement;
using System.Diagnostics;
using SmallBank.Grains.State;
using Concurrency.Common.ICache;
using Orleans.GrainDirectory;

namespace SmallBank.Grains;

[Reentrant]
[SnapperGrainPlacementStrategy]
[GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
public class OrleansTransactionalAccountGrain : Grain, IOrleansTransactionalAccountGrain
{
    readonly string grainName = typeof(OrleansTransactionalAccountGrain).FullName;

    string myRegionID;
    string mySiloID;
    SnapperGrainID myID;
    readonly ISnapperClusterCache snapperClusterCache;

    readonly ITransactionalState<Dictionary<UserID, AccountInfo>> users;
    readonly ITransactionalState<Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>> followersInfo;
    readonly ITransactionalState<Dictionary<UserID, UserID>> originInfo;

    public OrleansTransactionalAccountGrain(
        ISnapperClusterCache snapperClusterCache,
        [TransactionalState(nameof(users))] ITransactionalState<Dictionary<UserID, AccountInfo>> users,
        [TransactionalState(nameof(followersInfo))] ITransactionalState<Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>> followersInfo,
        [TransactionalState(nameof(originInfo))] ITransactionalState<Dictionary<UserID, UserID>> originInfo)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.users = users ?? throw new ArgumentNullException(nameof(users));
        this.followersInfo = followersInfo ?? throw new ArgumentNullException(nameof(followersInfo));
        this.originInfo = originInfo ?? throw new ArgumentNullException(nameof(originInfo));
    }

    public async Task<TransactionResult> Init(object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        myID = new SnapperGrainID(Guid.Parse(strs[0]), strs[1] + '+' + strs[2], grainName);
        myRegionID = strs[1];
        mySiloID = strs[2];
        Debug.Assert(strs[2] == RuntimeIdentity);

        Debug.Assert(obj != null);
        var numAccounts = (int)obj;

        await users.PerformUpdate(x => x = new Dictionary<UserID, AccountInfo>());
        await users.PerformUpdate(x =>
        {
            for (var i = 0; i < numAccounts; i++)
            {
                var userID = SmallBankIdMapping.GetUserID(myID.grainID, i);
                x.Add(userID, new AccountInfo(1000000));
            }
        });

        await followersInfo.PerformUpdate(x => x = new Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>());

        await originInfo.PerformUpdate(x => x = new Dictionary<UserID, UserID>());

        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);
        return txnResult;
    }

    public async Task<TransactionResult> ReadState(object? obj = null)
    {
        var txnResult = new TransactionResult();
        var item1 = await users.PerformRead(x => x);
        var item2 = await followersInfo.PerformRead(x => x);
        var item3 = await originInfo.PerformRead(x => x);
        txnResult.SetResult(SmallBankSerializer.SerializeAccountGrainState(item1, item2, item3));
        return txnResult;
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
        await users.PerformUpdate(x =>
        {
            if (writeInfoPerGrain.ContainsKey(myID.grainID))
            {
                foreach ((var fromUser, var moneyToWithdraw) in writeInfoPerGrain[myID.grainID])
                    if (x.ContainsKey(fromUser)) x[fromUser].balance -= moneyToWithdraw;
                
                writeInfoPerGrain.Remove(myID.grainID);
            }
        });

        // do read on the current grain
        var total = await users.PerformRead(x => 
        {
            var total = 0.0;
            if (readInfoPerGrain.ContainsKey(myID.grainID))
            {
                foreach (var user in readInfoPerGrain[myID.grainID])
                    total += x.ContainsKey(user) ? x[user].balance : 0.0;

                readInfoPerGrain.Remove(myID.grainID);
            }

            return total;
        });

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
        var toGrains = depositInputPerGrain.Keys.ToList();
        if (Constants.noDeadlock) toGrains.Sort();
        var tasks = new List<Task<TransactionResult>>();
        foreach (var grainID in toGrains)
        {
            Debug.Assert(!grainID.Equals(myID.grainID));

            var grain = GrainReferenceHelper.GetGrainReference<IOrleansTransactionalAccountGrain>(snapperClusterCache, GrainFactory, myRegionID, grainID);

            if (Constants.noDeadlock) await grain.Deposit(depositInputPerGrain[grainID]);
            else tasks.Add(grain.Deposit(depositInputPerGrain[grainID]));
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

    public async Task<TransactionResult> Deposit(object? obj = null)
    {
        var time = DateTime.UtcNow;
        var txnResult = new TransactionResult();
        txnResult.AddGrain(myID);

        Debug.Assert(obj != null);
        var input = obj as DepositInput;
        Debug.Assert(input != null);

        // do write on the current grain
        await users.PerformUpdate(x => 
        {
            foreach (var item in input.writeInfo)
            {
                var userID = item.Key;
                var moneyToDeposit = item.Value;

                if (x.ContainsKey(userID)) x[userID].balance += moneyToDeposit;
            }
        });

        // do read on the current grain
        var total = await users.PerformRead(x => 
        {
            var total = 0.0;
            foreach (var user in input.readInfo) total += x.ContainsKey(user) ? x[user].balance : 0.0;
            return total;
        });

        txnResult.SetResult((DateTime.UtcNow - time).TotalMilliseconds);
        return txnResult;
    }
}
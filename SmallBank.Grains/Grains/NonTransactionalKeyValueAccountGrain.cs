using SmallBank.Interfaces;
using System.Diagnostics;
using Concurrency.Common;
using Concurrency.Common.ICache;
using SmallBank.Grains.State;
using Concurrency.Implementation.DataModel;

namespace SmallBank.Grains;

public class NonTransactionalKeyValueAccountGrain : NonTransactionalKeyValueGrain, INonTransactionalKeyValueAccountGrain
{
    readonly string grainName = typeof(NonTransactionalKeyValueAccountGrain).FullName;

    string myRegionID;
    string mySiloID;
    
    public NonTransactionalKeyValueAccountGrain(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache)
        : base(typeof(NonTransactionalKeyValueAccountGrain).FullName, snapperClusterCache, snapperReplicaCache) { }

    /// <summary> this is only for initialization </summary>
    public async Task<object?> Init(SnapperID tid, object? obj = null)
    {
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        var numAccounts = (int)obj;

        (var dictionaryState, _) = GetState(tid);
        
        for (var i = 0; i < numAccounts; i++)
        {
            var userID = SmallBankIdMapping.GetUserID(myID.grainID, i);
            dictionaryState.Put(userID, new AccountInfo(1000000));
        }
        
        return null;
    }

    public async Task<object?> MultiTransfer(SnapperID tid, object? obj = null)
    {
        Debug.Assert(obj != null);
        var input = obj as MultiTransferInput;
        Debug.Assert(input != null);

        var writeInfoPerGrain = input.GetWriteInfoPerGrain(grainName);
        var readInfoPerGrain = input.GetReadInfoPerGrain(grainName);

        (var dictionaryState, _) = GetState(tid);

        // do write on the current grain
        if (writeInfoPerGrain.ContainsKey(myID.grainID))
        {
            foreach ((var fromUser, var moneyToWithdraw) in writeInfoPerGrain[myID.grainID])
            {
                var accountInfo = dictionaryState.Get(fromUser) as AccountInfo;
                if (accountInfo == null) continue;

                accountInfo.balance -= moneyToWithdraw;
                dictionaryState.Put(fromUser, accountInfo);
            }

            writeInfoPerGrain.Remove(myID.grainID);
        }

        // do read on the current grain
        var total = 0.0;
        if (readInfoPerGrain.ContainsKey(myID.grainID))
        {
            foreach (var user in readInfoPerGrain[myID.grainID])
            {
                var accountInfo = dictionaryState.Get(user) as AccountInfo;
                if (accountInfo == null) continue;

                total += accountInfo.balance;
            }

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
        var method = typeof(NonTransactionalKeyValueAccountGrain).GetMethod("Deposit");
        Debug.Assert(method != null);
        var tasks = new List<Task<object?>>();
        foreach (var info in depositInputPerGrain)
        {
            Debug.Assert(!info.Key.Equals(myID.grainID));

            var funcCall = new FunctionCall(method, info.Value);
            tasks.Add(CallGrain(info.Key, funcCall, tid));
        }
        await Task.WhenAll(tasks);

        var times = new List<double>();
        foreach (var t in tasks)
        {
            Debug.Assert(t.Result != null);
            times.Add((double)t.Result);
        }

        return times;
    }

    public async Task<object?> Deposit(SnapperID tid, object? obj = null)
    {
        var time = DateTime.UtcNow;
        await Task.CompletedTask;

        Debug.Assert(obj != null);
        var input = obj as DepositInput;
        Debug.Assert(input != null);

        (var dictionaryState, _) = GetState(tid);

        // do write on the current grain
        foreach (var item in input.writeInfo)
        {
            var userID = item.Key;
            var moneyToDeposit = item.Value;

            var accountInfo = dictionaryState.Get(userID) as AccountInfo;
            if (accountInfo == null) continue;

            accountInfo.balance += moneyToDeposit;
            dictionaryState.Put(userID, accountInfo);
        }

        // do read on the current grain
        var total = 0.0;
        foreach (var userID in input.readInfo)
        {
            var accountInfo = dictionaryState.Get(userID) as AccountInfo;
            if (accountInfo == null) continue;

            total += accountInfo.balance;
        }

        return (DateTime.UtcNow - time).TotalMilliseconds;
    }
}
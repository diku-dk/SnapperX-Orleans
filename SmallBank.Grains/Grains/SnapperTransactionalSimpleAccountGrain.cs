using Utilities;
using SmallBank.Interfaces;
using System.Diagnostics;
using Concurrency.Common;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using SmallBank.Grains.State;

namespace SmallBank.Grains;

public class SnapperTransactionalSimpleAccountGrain : TransactionExecutionGrain, ISnapperTransactionalAccountGrain
{
    readonly string grainName = typeof(SnapperTransactionalSimpleAccountGrain).FullName;

    GeneralStringKey myKey = new GeneralStringKey();

    GeneralStringKey GetMyKey()
    {
        if (string.IsNullOrEmpty(myKey.key)) myKey = new GeneralStringKey("SimpleAccountGrain");
        return myKey;
    }

    public SnapperTransactionalSimpleAccountGrain(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalSimpleAccountGrain).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    /// <summary> this is only for initialization </summary>
    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var numAccounts = (int)obj;

        (var dictionaryState, _) = await GetSimpleState(AccessMode.ReadWrite, cxt);

        var users = new Dictionary<UserID, AccountInfo>();
        for (var i = 0; i < numAccounts; i++)
        {
            var userID = SmallBankIdMapping.GetUserID(myID.grainID, i);
            users.Add(userID, new AccountInfo(1000000));
        }

        dictionaryState.Put(GetMyKey(), new SimpleAccountGrainState(users, new Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>(), new Dictionary<UserID, UserID>()));
        
        return null;
    }

    public async Task<object?> ReadSimpleState(TransactionContext cxt, object? obj = null)
    {
        (var dictionaryState, _) = await GetSimpleState(AccessMode.ReadWrite, cxt);
        var accountGrainState = dictionaryState.Get(GetMyKey()) as SimpleAccountGrainState;
        Debug.Assert(accountGrainState != null);

        return SmallBankSerializer.SerializeAccountGrainState(accountGrainState.users, accountGrainState.followersInfo, accountGrainState.originInfo);
    }

    public async Task<object?> MultiTransfer(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var input = obj as MultiTransferInput;
        Debug.Assert(input != null);

        var writeInfoPerGrain = input.GetWriteInfoPerGrain(grainName);
        var readInfoPerGrain = input.GetReadInfoPerGrain(grainName);

        AccessMode accessModeOnGrain;
        if (writeInfoPerGrain.ContainsKey(myID.grainID)) accessModeOnGrain = AccessMode.ReadWrite;
        else
        {
            Debug.Assert(readInfoPerGrain.ContainsKey(myID.grainID));
            accessModeOnGrain = AccessMode.Read;
        }

        (var dictionaryState, _) = await GetSimpleState(accessModeOnGrain, cxt);
        var accountGrainState = dictionaryState.Get(GetMyKey()) as SimpleAccountGrainState;
        Debug.Assert(accountGrainState != null);

        // do write on the current grain
        if (writeInfoPerGrain.ContainsKey(myID.grainID))
        {
            foreach ((var fromUser, var moneyToWithdraw) in writeInfoPerGrain[myID.grainID])
            {
                if (accountGrainState.users.ContainsKey(fromUser))
                {
                    var accountInfo = accountGrainState.users[fromUser];
                    accountInfo.balance -= moneyToWithdraw;
                }
            }

            writeInfoPerGrain.Remove(myID.grainID);
        }

        // do read on the current grain
        var total = 0.0;
        if (readInfoPerGrain.ContainsKey(myID.grainID))
        {
            foreach (var user in readInfoPerGrain[myID.grainID])
            {
                if (accountGrainState.users.ContainsKey(user))
                    total += accountGrainState.users[user].balance;
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
        var method = typeof(SnapperTransactionalSimpleAccountGrain).GetMethod("Deposit");
        Debug.Assert(method != null);
        var tasks = new List<Task<object?>>();
        foreach (var info in depositInputPerGrain)
        {
            Debug.Assert(!info.Key.Equals(myID.grainID));

            var funcCall = new FunctionCall(method, info.Value);
            tasks.Add(CallGrain(info.Key, funcCall, cxt));
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

    public async Task<object?> Deposit(TransactionContext cxt, object? obj = null)
    {
        var time = DateTime.UtcNow;
        Debug.Assert(obj != null);
        var input = obj as DepositInput;
        Debug.Assert(input != null);

        AccessMode accessModeOnGrain;
        if (input.writeInfo.Count != 0) accessModeOnGrain = AccessMode.ReadWrite;
        else
        {
            Debug.Assert(input.readInfo.Count != 0);
            accessModeOnGrain = AccessMode.Read;
        }

        (var dictionaryState, _) = await GetSimpleState(accessModeOnGrain, cxt);
        var accountGrainState = dictionaryState.Get(GetMyKey()) as SimpleAccountGrainState;
        Debug.Assert(accountGrainState != null);

        // do write on the current grain
        foreach (var item in input.writeInfo)
        {
            var userID = item.Key;
            var moneyToDeposit = item.Value;
            if (accountGrainState.users.ContainsKey(userID))
                accountGrainState.users[userID].balance += moneyToDeposit;
        }

        // do read on the current grain
        var total = 0.0;
        foreach (var userID in input.readInfo)
        {
            if (accountGrainState.users.ContainsKey(userID))
                total += accountGrainState.users[userID].balance;
        }

        return (DateTime.UtcNow - time).TotalMilliseconds;
    }
}
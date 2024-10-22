using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface;
using Concurrency.Common;
using TestApp.Interfaces;
using System.Diagnostics;
using Utilities;
using Concurrency.Common.ILogging;
using Concurrency.Common.ICache;
using TestApp.Grains.State;
using Concurrency.Common.State;

namespace TestApp.Grains;

public class SnapperTransactionalTestGrain : TransactionExecutionGrain, ISnapperTransactionalTestGrain
{
    Random rnd = new Random();
    TestKeyV1 myKey1 = new TestKeyV1();
    TestKeyV2 myKey2 = new TestKeyV2();

    public SnapperTransactionalTestGrain(ISnapperClusterCache snapperClusterCache, ISnapperReplicaCache snapperReplicaCache, IScheduleManager scheduleManager, ISnapperLoggingHelper log)
        : base(typeof(SnapperTransactionalTestGrain).FullName, snapperClusterCache, snapperReplicaCache, scheduleManager, log) { }

    public async Task<object?> Init(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var actorID = (Guid)obj;
        var strs = this.GetPrimaryKeyString().Split('+');
        Debug.Assert(actorID == Guid.Parse(strs[0]));

        (var dictionaryState, var listState) = await GetState(AccessMode.ReadWrite, cxt);
        myKey1 = new TestKeyV1(Helper.ConvertGuidToInt(myID.grainID.id), "key");
        dictionaryState.Put(myKey1, new TestValue(100.0, 100.0));
        
        myKey2 = new TestKeyV2(Helper.ConvertGuidToInt(myID.grainID.id), "info", "note");
        dictionaryState.Put(myKey2, new TestValue(200.0, 200.0));
        listState.Add(new TestLog($"{DateTime.UtcNow}: init state"));
        return null;
    }

    public async Task<object?> DoOp(TransactionContext cxt, object? obj = null)
    {
        var strs = this.GetPrimaryKeyString().Split('+');
        var myGrainID = new GrainID(Guid.Parse(strs[0]), typeof(SnapperTransactionalTestGrain).FullName);

        Debug.Assert(obj != null);
        var input = obj as TestFuncInput;
        Debug.Assert(input != null);
        Debug.Assert(input.info.ContainsKey(myGrainID));
        Debug.Assert(input.info.Count() == 1);

        // do operation on the current grain
        var info = input.info.First().Value;
        var accessOnCurrentGrain = info.Item1;
        switch (accessOnCurrentGrain)
        {
            case AccessMode.Read:
                (var _, var _) = await GetState(accessOnCurrentGrain, cxt);
                break;
            case AccessMode.ReadWrite:
                var opOnCurrentGrain = info.Item2;
                ISnapperKey targetKey = rnd.Next(0, 100) < 50 ? myKey1 : myKey2;
                await DoWrite(opOnCurrentGrain, targetKey, cxt);
                break;
        }

        // call other grains if needed
        var task = new List<Task>();
        foreach (var grainsInfo in info.Item3.info)
        {
            Debug.Assert(!grainsInfo.Key.Equals(myGrainID));
            var method = typeof(SnapperTransactionalTestGrain).GetMethod("DoOp");
            Debug.Assert(method != null);
            var newInput = new Dictionary<GrainID, (AccessMode, OperationType, TestFuncInput)> { { grainsInfo.Key, (grainsInfo.Value.Item1, grainsInfo.Value.Item2, grainsInfo.Value.Item3) } };
            var funcCall = new FunctionCall(method, new TestFuncInput(newInput));
            task.Add(CallGrain(grainsInfo.Key, funcCall, cxt));
        }
        await Task.WhenAll(task);
        return null;
    }

    async Task DoWrite(OperationType opOnCurrentGrain, ISnapperKey targetKey, TransactionContext cxt)
    {
        (var dictionaryState, var listState) = await GetState(AccessMode.ReadWrite, cxt);
        var v = dictionaryState.Get(targetKey);

        switch (opOnCurrentGrain)
        {
            case OperationType.Update:
                if (v != null)
                {
                    var myValue = v as TestValue;
                    Debug.Assert(myValue != null);

                    var oldBalance = myValue.balance;
                    var oldSaving = myValue.saving;
                    if (rnd.Next(0, 100) < 50) myValue.balance += rnd.Next(0, 100);
                    else
                    {
                        var money = rnd.Next(0, 100);
                        if (myValue.balance < money) myValue.saving -= money;
                        else myValue.balance -= money;
                    }

                    (var updated, var msg) = dictionaryState.Put(targetKey, myValue);  // must call "Put" to register the update on the value
                                                                                       //Console.WriteLine($"Grain {Helper.ConvertGuidToInt(myID.grainID.id)}: update key {targetKey.Print()}, updated = {updated}, msg = {msg}");
                    if (rnd.Next(0, 100) < 10)
                    {
                        var str = $"{DateTime.UtcNow}: change ({oldBalance}, {oldSaving}) => ({myValue.balance}, {myValue.saving})";
                        listState.Add(new TestLog(str));
                    }
                }
                else
                {
                    var value = new TestValue(rnd.Next(0, 100), rnd.Next(0, 100));
                    (var put, var msg) = dictionaryState.Put(targetKey, value);
                    //Console.WriteLine($"Grain {Helper.ConvertGuidToInt(myID.grainID.id)}: put key {targetKey.Print()}, put = {put}, msg = {msg}");
                    if (rnd.Next(0, 100) < 10)
                    {
                        var str = $"{DateTime.UtcNow}: add key = {targetKey.Print()}, value = {value.Print()}";
                        listState.Add(new TestLog(str));
                    }
                }
                break;
            case OperationType.Delete:
                if (v != null)
                {
                    (var removed, var msg) = dictionaryState.Delete(targetKey);
                    //Console.WriteLine($"Grain {Helper.ConvertGuidToInt(myID.grainID.id)}: remove key {targetKey.Print()}, removed = {removed}, msg = {msg}");
                    break;
                }
                break;
        }
    }

    public async Task<object?> ReadState(TransactionContext cxt, object? obj = null) => await TakeSnapshotOfGrainState(cxt);

    public async Task<object?> RegisterUpdateReference(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null);

        return await RegisterReference(cxt, referenceInfo);
    }

    public async Task<object?> RegisterDeleteReference(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null);

        return await RegisterReference(cxt, referenceInfo);
    }

    public async Task<object?> DeRegisterUpdateReference(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        (var dictionaryState, var _) = await GetState(AccessMode.ReadWrite, cxt);
        dictionaryState.Delete(referenceInfo.key2);
        return null;
    }

    public async Task<object?> DeRegisterDeleteReference(TransactionContext cxt, object? obj = null)
    {
        Debug.Assert(obj != null);
        var referenceInfo = obj as ReferenceInfo;
        Debug.Assert(referenceInfo != null && referenceInfo.referenceType != SnapperKeyReferenceType.NoReference);

        (var dictionaryState, var _) = await GetState(AccessMode.ReadWrite, cxt);
        dictionaryState.Delete(referenceInfo.key2);
        return null;
    }
}
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.Scheduling;

public class ScheduleNode
{
    readonly SnapperGrainID myGrainID;
    readonly bool speculativeACT;
    readonly bool speculativeBatch;

    public bool isDet;
    public ScheduleNode? prev;
    public ScheduleNode? next;

    /// <summary> the task is set true when the bacth info arrives </summary>
    public TaskCompletionSource waitForBatchInfo;

    /// <summary> set result when a batch completes </summary>
    public TaskCompletionSource waitForCompletion;

    // ============================================================================================ for det node
    public Batch batch;
    /// <summary> count the number of PACTs that haven't been executed on this grain </summary>
    public CountdownEvent count;
    /// <summary> set result when a batch commits </summary>
    public TaskCompletionSource waitForCommit;
    /// <summary> tid, the task is completed when the transaction finishes execution on the grain </summary>
    public Dictionary<SnapperID, TaskCompletionSource> waitTxnComplete;

    // ======================================================================================== for non-det node
    public SnapperID tid;
    public HashSet<SnapperID> transactions;

    public ScheduleNode(SnapperGrainID myGrainID, bool speculativeACT, bool speculativeBatch, Batch? batch = null)
    {
        this.myGrainID = myGrainID;
        this.speculativeACT = speculativeACT;
        this.speculativeBatch = speculativeBatch;

        isDet = true;
        prev = null;
        next = null;

        waitForBatchInfo = new TaskCompletionSource();
        waitForCompletion = new TaskCompletionSource();
        waitForCommit = new TaskCompletionSource();
        waitTxnComplete = new Dictionary<SnapperID, TaskCompletionSource>();

        if (batch != null) SetBatchInfo(batch);
        else batch = new Batch();

        tid = new SnapperID();
        transactions = new HashSet<SnapperID>();
    }

    public ScheduleNode(SnapperGrainID myGrainID, bool speculativeACT, bool speculativeBatch, SnapperID tid)
    {
        this.myGrainID = myGrainID;
        this.speculativeACT = speculativeACT;
        this.speculativeBatch = speculativeBatch;

        isDet = false;
        prev = null;
        next = null;

        waitForBatchInfo = new TaskCompletionSource();
        waitForCompletion = new TaskCompletionSource();
        waitForCommit = new TaskCompletionSource();
        waitTxnComplete = new Dictionary<SnapperID, TaskCompletionSource>();

        batch = new Batch();
        count = new CountdownEvent(0);

        this.tid = tid;
        transactions = new HashSet<SnapperID>();
    }

    /// <summary> this is called when the node is created before the batch info arrives </summary>
    public void SetBatchInfo(Batch batch)
    {
        this.batch = batch;

        var numtxn = batch.txnList.Count;
        if (numtxn == 0) return;
        
        count = new CountdownEvent(numtxn);

        foreach (var item in batch.txnList) waitTxnComplete.Add(item.Item1, new TaskCompletionSource());
    }

    public async Task WaitForTurn(TransactionContext cxt)
    {
        Debug.Assert(cxt.bid.Equals(batch.GetPrimaryBid()));
        Debug.Assert(prev != null);

        if (!cxt.isDet)
        {
            var t = prev.waitForCommit.Task;
            
            // when the first task is completed, the other tasks will continue running until completion
            // if that behavior is not desired you may want to cancel all the remaining tasks once the first task complete
            // https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task.whenany?view=net-7.0
            await Task.WhenAny(t, Task.Delay(Constants.deadlockTimeout));
            if (!t.IsCompleted) throw new SnapperException(ExceptionType.NotSerializable);
        }
        else
        {
            // This is a PACT
            
            Task t = WaitForTurnForPACT(cxt.tid);

            await Task.WhenAny(t, Task.Delay(TimeSpan.FromSeconds(30)));
            if (!t.IsCompleted)
            {
                Console.WriteLine($"grain {myGrainID.grainID.Print()}: PACT tid = {cxt.tid.Print()}, bid = {cxt.bid.Print()}");
                throw new Exception();
            }
        }
    }

    async Task WaitForTurnForPACT(SnapperID tid)
    {
        Debug.Assert(prev != null);

        var depTid = batch.GetDependentTransaction(tid);

        if (!depTid.isEmpty()) await waitTxnComplete[depTid].Task;
        else
        {
            if (!prev.isDet) await prev.waitForCommit.Task;
            else
            {
                if (speculativeBatch) await prev.waitForCompletion.Task;
                else await prev.waitForCommit.Task;
            } 
        }
    }
}
using Concurrency.Common.State;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.Scheduling;

public class ScheduleList
{
    readonly SnapperGrainID myGrainID;
    readonly ScheduleNode headNode;
    readonly bool speculativeACT;
    readonly bool speculativeBatch;
    readonly CommitInfo commitInfo;
    readonly ExperimentData experimentData;

    Dictionary<SnapperID, TaskCompletionSource> waitForBatchInfo;  // primary bid

    // batch info
    Dictionary<SnapperID, SnapperID> bidToLocalBid;      // primary bid ==> local bid
    Dictionary<SnapperID, ScheduleNode> detNodes;        // node ID: local bid

    // ACT info
    Dictionary<SnapperID, ScheduleNode> nonDetNodes;     // node ID: ACT tid
    Dictionary<SnapperID, SnapperID> nonDetTidToNodeID;

    public ScheduleList(SnapperGrainID myGrainID, bool speculativeACT, bool speculativeBatch, CommitInfo commitInfo, ExperimentData experimentData)
    {
        this.myGrainID = myGrainID;
        this.speculativeACT = speculativeACT;
        this.speculativeBatch = speculativeBatch;
        this.experimentData = experimentData;
        waitForBatchInfo = new Dictionary<SnapperID, TaskCompletionSource>();
        detNodes = new Dictionary<SnapperID, ScheduleNode>();
        bidToLocalBid = new Dictionary<SnapperID, SnapperID>();
        nonDetNodes = new Dictionary<SnapperID, ScheduleNode>();
        nonDetTidToNodeID = new Dictionary<SnapperID, SnapperID>();

        this.commitInfo = commitInfo;

        headNode = new ScheduleNode(myGrainID, speculativeACT, speculativeBatch, new Batch());
        detNodes.Add(new SnapperID(), headNode);
        headNode.waitForCommit.SetResult();
        headNode.waitForCompletion.SetResult();
        headNode.waitForBatchInfo.SetResult();
    }

    public void CheckGC()
    {
        var numNodes = 1;
        var node = headNode;
        while (node.next != null)
        {
            numNodes++;
            node = node.next;
        }
        if (numNodes > 1) Console.WriteLine($"ScheduleList: numNodes = {numNodes}");

        if (waitForBatchInfo.Count != 0) Console.WriteLine($"ScheduleList: waitForBatchInfo.Count = {waitForBatchInfo.Count}");

        if (detNodes.Count > 1) Console.WriteLine($"ScheduleList: detNodes.Count = {detNodes.Count}");
        if (bidToLocalBid.Count != 0) Console.WriteLine($"ScheduleList: bidToLocalBid.Count = {bidToLocalBid.Count}");

        if (nonDetNodes.Count > 0) Console.WriteLine($"ScheduleList: nonDetNodes.Count = {nonDetNodes.Count}");
        if (nonDetTidToNodeID.Count != 0) Console.WriteLine($"ScheduleList: grain {myGrainID.grainID.Print()}, nonDetTidToNodeID.Count = {nonDetTidToNodeID.Count}");
    }

    public async Task WaitForScheduleInfo(TransactionContext cxt)
    {
        // This is an ACT, wait for previous batch to complete
        if (!cxt.isDet) RegisterACT(cxt);
        else
        {
            // This is a PACT (the bid is primary bid), wait for revious PACT to acquire lock

            // if the batch info hasn't arrived yet, need to wait
            if (!waitForBatchInfo.ContainsKey(cxt.bid)) waitForBatchInfo.Add(cxt.bid, new TaskCompletionSource());
            var promise = waitForBatchInfo[cxt.bid];
            await promise.Task;
        }
    }

    public async Task WaitForTurn(TransactionContext cxt, HashSet<ISnapperKey>? keys = null)
    {
        ScheduleNode node;
        if (!cxt.isDet)
        {
            // This is an ACT
            Debug.Assert(nonDetTidToNodeID.ContainsKey(cxt.tid));
            node = nonDetNodes[nonDetTidToNodeID[cxt.tid]];
            await node.WaitForTurn(cxt);
        }
        else
        {
            // This is a PACT (the bid is primary bid)
            Debug.Assert(bidToLocalBid.ContainsKey(cxt.bid));
            var localBid = bidToLocalBid[cxt.bid];
            Debug.Assert(detNodes.ContainsKey(localBid));
            node = detNodes[localBid];

            // need to wait for all previous ACT node to commit
            var depNode = node.prev;
            while (depNode != null)
            {
                if (!depNode.isDet) await depNode.waitForCommit.Task;
                depNode = depNode.prev;
            }

            if (keys == null)
            {
                try
                {
                    await node.WaitForTurn(cxt);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"grain {myGrainID.grainID.Print()}: prev node isDet = {node.prev.isDet}, bid = {node.prev.batch.GetPrimaryBid().Print()}, {e.Message}, {e.StackTrace}");
                    var currentNode = node;
                    while (currentNode.prev != null)
                    {
                        if (currentNode.isDet) Console.WriteLine($"grain {myGrainID.grainID.Print()}: primary bid = {currentNode.batch.GetPrimaryBid().Print()}, completed = {currentNode.waitForCompletion.Task.IsCompleted}, committed = {currentNode.waitForCommit.Task.IsCompleted}");
                        else Console.WriteLine($"grain {myGrainID.grainID.Print()}: act node id = {currentNode.tid.Print()}, completed = {currentNode.waitForCompletion.Task.IsCompleted}, committed = {currentNode.waitForCommit.Task.IsCompleted}");
                        currentNode = currentNode.prev;
                    }
                    Debug.Assert(false);
                }
            }
            else
            {
                var depInfo = new Dictionary<SnapperID, HashSet<SnapperID>>();
                foreach (var key in keys)
                {
                    (var depBid, var depTid) = node.batch.GetDependentTransactionOnKey(key, cxt.tid);

                    if (!depBid.isEmpty())
                    {
                        if (!depInfo.ContainsKey(depBid)) depInfo.Add(depBid, new HashSet<SnapperID>());
                        depInfo[depBid].Add(depTid);
                    }
                }

                // resolve dependencies between PACT
                foreach (var item in depInfo) if (!commitInfo.isBatchCommit(item.Key)) await WaitForDependency(item.Key, item.Value, cxt);
            }
        }
    }

    async Task WaitForDependency(SnapperID depBid, HashSet<SnapperID> depTids, TransactionContext cxt)
    {
        if (commitInfo.isBatchCommit(depBid)) return;

        if (!waitForBatchInfo.ContainsKey(depBid)) waitForBatchInfo.Add(depBid, new TaskCompletionSource());
        var promise = waitForBatchInfo[depBid];
        await promise.Task;

        if (commitInfo.isBatchCommit(depBid)) return;

        var depLocalBid = bidToLocalBid[depBid];
        var depNode = detNodes[depLocalBid];

        var tasks = new List<Task>();
        foreach (var depTid in depTids)
        {
            if (speculativeBatch) tasks.Add(depNode.waitTxnComplete[depTid].Task);
            else
            {
                if (depBid.Equals(cxt.bid)) tasks.Add(depNode.waitTxnComplete[depTid].Task);
                else tasks.Add(depNode.waitForCommit.Task);
            }
        }
        await Task.WhenAll(tasks);
    }

    /// <summary> remove a node from the schedule list </summary>
    void RemoveNode(ScheduleNode node)
    {
        var prev = node.prev;
        Debug.Assert(prev != null);

        if (node.next != null)
        {
            node.next.prev = prev;
            prev.next = node.next;
        }
        else prev.next = null;
    }

    ScheduleNode FindTail()
    {
        var node = headNode;
        while (node.next != null) node = node.next;
        return node;
    }

    public async Task WaitForBatchCommit(SnapperID primaryBid)
    {
        if (!bidToLocalBid.ContainsKey(primaryBid)) return;

        if (commitInfo.isBatchCommit(primaryBid)) return;

        var localBid = bidToLocalBid[primaryBid];
        var node = detNodes[localBid];
        await node.waitForCommit.Task;
    }

    // ======================================================================================================================================= For PACT ====
    /// <summary> input: committed primary bid </summary>
    public void CommitBatch(SnapperID primaryBid)
    {
        var localBid = bidToLocalBid[primaryBid];
        var node = detNodes[localBid];
        Debug.Assert(!node.batch.GetPrimaryBid().isEmpty());

        RemoveNode(node);

        bidToLocalBid.Remove(primaryBid);
        detNodes.Remove(localBid);
        waitForBatchInfo.Remove(primaryBid);

        commitInfo.CommitBatch(primaryBid);
        node.waitForCommit.SetResult();
        Debug.Assert(node.waitForCompletion.Task.IsCompleted);
    }

    public void RegisterBatch(Batch batch)
    {
        var time = DateTime.UtcNow;
        var localBid = batch.local_bid;
        var prevLocalBid = batch.local_prevBid;

        var primaryBid = batch.GetPrimaryBid();
        var prevLocalPrimaryBid = batch.local_prev_primaryBid;
        
        ScheduleNode node;
        if (!detNodes.ContainsKey(localBid))
        {
            node = new ScheduleNode(myGrainID, speculativeACT, speculativeBatch, batch);
            detNodes.Add(localBid, node);
        }
        else
        {
            node = detNodes[localBid];
            node.SetBatchInfo(batch);
        }

        bidToLocalBid[primaryBid] = localBid;

        if (detNodes.ContainsKey(prevLocalBid))
        {
            var prevNode = detNodes[prevLocalBid];

            if (prevNode.next == null)
            {
                prevNode.next = node;
                node.prev = prevNode;
            }
            else
            {
                Debug.Assert(!prevNode.next.isDet);
                Debug.Assert(prevNode.next.next == null);
                prevNode.next.next = node;
                node.prev = prevNode.next;
            }
        }
        else
        {
            // last node is already deleted because it's committed
            if (commitInfo.isBatchCommit(prevLocalPrimaryBid))
            {
                var tail = FindTail();
                tail.next = node;
                node.prev = tail;
            }
            else   // last node hasn't arrived yet
            {
                var prevNode = new ScheduleNode(myGrainID, speculativeACT, speculativeBatch);
                detNodes.Add(prevLocalBid, prevNode);
                prevNode.next = node;
                node.prev = prevNode;
            }
        }

        Debug.Assert(!node.waitForBatchInfo.Task.IsCompleted);
        node.waitForBatchInfo.SetResult();

        // mark that the schedule has been added to the grain schedule
        if (!waitForBatchInfo.ContainsKey(primaryBid)) waitForBatchInfo.Add(primaryBid, new TaskCompletionSource());
        waitForBatchInfo[primaryBid].SetResult();

        experimentData.Set(MonitorTime.RegisterBatchOnGrain, (DateTime.UtcNow - time).TotalMilliseconds);
    }

    /// <summary> A PACT completes when it finishes execution on a grain. </summary>
    /// <returns> If the schedule node has all transactions completed, the coordinator who emitted this batch </returns>
    public (bool, SnapperGrainID?) CompleteTxn(TransactionContext cxt)
    {
        Debug.Assert(cxt.isDet);
        var primaryBid = cxt.bid;
        var localBid = bidToLocalBid[primaryBid];
        var node = detNodes[localBid];

        node.waitTxnComplete[cxt.tid].SetResult();

        // if all PACTs have completed on this grain
        if (node.count.Signal())
        {
            node.waitForCompletion.SetResult();
            return (true, node.batch.coordID);
        }
        else return (false, null);
    }

    // ======================================================================================================================================= For ACT =====
    public void RegisterACT(TransactionContext cxt)
    {
        var tid = cxt.tid;
        if (nonDetTidToNodeID.ContainsKey(tid)) return;   // the ACT has been registered before

        var tail = FindTail();
        if (tail.isDet)
        {
            var node = new ScheduleNode(myGrainID, speculativeACT, speculativeBatch, tid);
            node.transactions.Add(tid);
            nonDetNodes.Add(tid, node);
            nonDetTidToNodeID.Add(tid, tid);
            tail.next = node;
            node.prev = tail;
        }
        else
        {
            Debug.Assert(tail.prev != null);
            Debug.Assert(tail.batch.GetPrimaryBid().isEmpty());

            // Join the non-det tail, replace the promise
            if (tail.waitForCompletion.Task.IsCompleted)
            {
                tail.waitForCompletion = new TaskCompletionSource();
                tail.waitForCommit = new TaskCompletionSource();
            }
            else Debug.Assert(!tail.waitForCommit.Task.IsCompleted);
            
            nonDetNodes[tail.tid].transactions.Add(tid);
            nonDetTidToNodeID.Add(tid, tail.tid);
        }
    }

    /// <summary> An ACT completes when it is committed or aborted. </summary>
    public void CompleteTxn(SnapperID tid)
    {
        if (!nonDetTidToNodeID.ContainsKey(tid)) return;

        var nodeID = nonDetTidToNodeID[tid];
        nonDetTidToNodeID.Remove(tid);

        var node = nonDetNodes[nodeID];
        
        if (node.transactions.Contains(tid))
        {
            node.transactions.Remove(tid);
            if (node.transactions.Count == 0)
            {
                RemoveNode(node);

                nonDetNodes.Remove(nodeID);
                node.waitForCompletion.SetResult();
                node.waitForCommit.SetResult();
            }
        }
    }
}
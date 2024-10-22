using Concurrency.Common;
using Concurrency.Common.Logging;
using Concurrency.Common.State;
using System.Collections.Concurrent;
using System.Diagnostics;
using Utilities;

namespace Replication.Implementation;

internal class HistoryList
{
    public readonly SnapperGrainID myGrainID;
    readonly HistoryNode headNode;

    /// <summary> each node is either a batch (key is primary bid) or an ACT (key is tid) </summary>
    ConcurrentDictionary<SnapperID, HistoryNode> nodes;

    /// <summary> for transaction execution </summary>
    ConcurrentDictionary<SnapperID, SnapperID> txnIDToNodeID;

    /// <summary> wait for the registration of a replica node </summary>
    ConcurrentDictionary<SnapperID, TaskCompletionSource> waitForRegistration;

    public HistoryList(SnapperGrainID myGrainID)
    { 
        this.myGrainID = myGrainID;
        var id = new SnapperID();
        headNode = new HistoryNode(myGrainID, id);
        headNode.waitForCommit.SetResult();

        nodes = new ConcurrentDictionary<SnapperID, HistoryNode>();
        var added = nodes.TryAdd(id, headNode);
        Debug.Assert(added);

        txnIDToNodeID = new ConcurrentDictionary<SnapperID, SnapperID>();
        waitForRegistration = new ConcurrentDictionary<SnapperID, TaskCompletionSource>();
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
        if (numNodes > 1) Console.WriteLine($"HistoryList: numNodes = {numNodes}");

        if (nodes.Count > 1) Console.WriteLine($"HistoryList: nodes.Count = {nodes.Count}");
        if (txnIDToNodeID.Count != 0) Console.WriteLine($"HistoryList: txnIDToNodeID.Count = {txnIDToNodeID.Count}");
        if (waitForRegistration.Count != 0) Console.WriteLine($"HistoryList: waitForRegistration.Count = {waitForRegistration.Count}");
    }

    // ================================================================================== for replication
    public async Task WaitForPrevToRegister(SnapperID prevId, DateTime prevTimestamp)
    {
        // the prev replica node has been registered
        if (nodes.ContainsKey(prevId)) return;

        // the prev replica node has been committed
        if (headNode.timestamp == prevTimestamp) return;

        // otherwise, need to wait for the prev replica node registration
        var promise = waitForRegistration.GetOrAdd(prevId, new TaskCompletionSource());
        await promise.Task;
    }

    public void RegisterGrainState(PrepareLog log, SnapperGrainStateUpdates updates)
    {
        Debug.Assert(log.grainID.id.Equals(myGrainID.grainID.id));
        Debug.Assert(Helper.GetReplicaClassName(log.grainID.className).Equals(myGrainID.grainID.className));
        Debug.Assert(!nodes.ContainsKey(log.id));

        var node = new HistoryNode(myGrainID, log.id);
        node.SetState(log.timestamp, updates);
        var added = nodes.TryAdd(node.id, node);
        Debug.Assert(added);

        // check prev node
        HistoryNode? prevNode;
        var found = nodes.TryGetValue(log.prevId, out prevNode);
        if (!found)
        {
            // the prev node has been removed because it is committed
            Debug.Assert(headNode.timestamp == log.prevTimestamp);

            prevNode = headNode;
        }

        Debug.Assert(prevNode != null);
        if (prevNode.next == null)
        {
            prevNode.next = node;
            node.prev = prevNode;
        }
        else
        {
            Debug.Assert(!prevNode.next.isReplica);
            Debug.Assert(prevNode.next.next == null);
            prevNode.next.next = node;
            node.prev = prevNode.next;
        }

        var promise = waitForRegistration.GetOrAdd(log.id, new TaskCompletionSource());
        promise.SetResult();
    }

    public async Task WaitForPrevToCommit(SnapperID id)
    {
        // get the current node
        HistoryNode? node;
        var found = nodes.TryGetValue(id, out node);
        Debug.Assert(found);
        Debug.Assert(node != null);

        // wait for previous node to commit first
        Debug.Assert(node.prev != null);
        //Console.WriteLine($"Grain {Helper.ConvertGuidToInt(myGrainID.id)}: try commit replica node, id = {node.id.id}, wait prev id = {node.prev.id.id}, isReplica = {node.prev.isReplica}");
        var isPrevReplica = node.prev.isReplica;
        await node.prev.waitForCommit.Task;

        // node's previous one may be a node that aborts all ACTs
        // after the commit of the original previous node, the current node is attached to a new prev node
        // this new prev node must be a replica node, and now we need to wait for commit again
        if (!isPrevReplica)
        {
            Debug.Assert(node.prev != null);
            Debug.Assert(node.prev.isReplica);

            // in this case, need to wait for prev's prev
            await node.prev.waitForCommit.Task;
        }
    }

    // ==================================================================== for read-only transactions on replica
    /// <summary> For ACT, append it to the tail </summary>
    public void RegisterACT(SnapperID tid)
    {
        bool added;
        HistoryNode node;
        var tail = FindTail();
        if (tail.isReplica)
        {
            // create a new node 
            node = new HistoryNode(myGrainID, tid, false);
            added = nodes.TryAdd(node.id, node);
            Debug.Assert(added);

            // append the new node to the tail node
            tail.next = node;
            node.prev = tail;
        }
        else
        { 
            node = tail;
            Debug.Assert(!node.waitForCommit.Task.IsCompleted);
        }

        // add transaction to the node
        added = txnIDToNodeID.TryAdd(tid, node.id);
        Debug.Assert(added);

        node.AddACT(tid);
    }

    /// <summary> For PACT, insert them right after the head node, which is already committed </summary>
    public void RegisterBatch(SnapperID primaryBid, List<SnapperID> tids)
    {
        bool added;
        HistoryNode node;
        var tail = FindTail();
        if (tail.isReplica)
        {
            // create and add a new node
            node = new HistoryNode(myGrainID, tids.First(), false);
            added = nodes.TryAdd(node.id, node);
            Debug.Assert(added);

            // append the node to the tail node
            node.prev = tail;
            tail.next = node;
        }
        else
        {
            node = tail;
            Debug.Assert(!node.waitForCommit.Task.IsCompleted);
        }

        tids.ForEach(tid =>
        {
            added = txnIDToNodeID.TryAdd(tid, node.id);
            Debug.Assert(added);
        });

        node.AddPACTs(primaryBid, tids);
        Debug.Assert(node.prev != null);
    }

    public async Task<SnapperGrainState> WaitForTurn(TransactionContext cxt)
    {
        var tid = cxt.tid;
        Debug.Assert(txnIDToNodeID.ContainsKey(tid));
        var node = nodes[txnIDToNodeID[tid]];

        // if it's an ACT, it may have not-serializable issue
        if (!cxt.isDet)
        {
            var t = node.WaitForTurn();
            await Task.WhenAny(t, Task.Delay(Constants.deadlockTimeout));
            if (!t.IsCompleted) throw new SnapperException(ExceptionType.NotSerializable);
        }
        else await node.WaitForTurn();
        
        Debug.Assert(node.prev != null);
        Debug.Assert(node.prev.id.isEmpty());  // now its prev must be head node
        return headNode.GetState();
    }

    /// <returns> if the whole batch has completed on the grain </returns>
    public bool CompleteTxn(TransactionContext cxt)
    {
        var tid = cxt.tid;
        if (!txnIDToNodeID.ContainsKey(tid)) return false;

        var nodeID = txnIDToNodeID[tid];
        var removed = txnIDToNodeID.TryRemove(tid, out _);
        Debug.Assert(removed);

        var node = nodes[nodeID];
        Debug.Assert(!node.isReplica);
        (var allComplete, var batchComplete) = node.CompleteTxn(cxt);
        if (allComplete) Commit(node.id);

        return batchComplete;
    }

    // ================================================================================== for both
    public void Commit(SnapperID nodeID)
    {
        // get the current node
        HistoryNode? node;
        var found = nodes.TryGetValue(nodeID, out node);
        Debug.Assert(found);
        Debug.Assert(node != null);
        Debug.Assert(node.prev != null);

        if (node.isReplica)
        {
            // at this moment, the prev node must be head node
            Debug.Assert(node.prev.id.isEmpty());
            Debug.Assert(headNode.timestamp < node.timestamp);

            // merge the state to the head node
            headNode.MergeState(node);
        }
        
        RemoveNode(node);
    }

    void RemoveNode(HistoryNode node)
    {
        Debug.Assert(node.prev != null);
        var prevNode = node.prev;

        if (node.next != null)
        {
            prevNode.next = node.next;
            node.next.prev = prevNode;
        }
        else prevNode.next = null;

        var removed = nodes.TryRemove(node.id, out _);
        Debug.Assert(removed);

        Debug.Assert(!node.waitForCommit.Task.IsCompleted);
        node.waitForCommit.SetResult();

        if (node.isReplica)
        {
            TaskCompletionSource? promise;
            var found = waitForRegistration.TryGetValue(node.id, out promise);
            Debug.Assert(found);
            Debug.Assert(promise != null);
            Debug.Assert(promise.Task.IsCompleted);
            removed = waitForRegistration.TryRemove(node.id, out _);
            Debug.Assert(removed);

            Debug.Assert(node.prev.waitForCommit.Task.IsCompleted);
        }
    }

    HistoryNode FindTail()
    {
        var node = headNode;
        while (node.next != null) node = node.next;
        Debug.Assert(node != null);
        return node;
    }
}
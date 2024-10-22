using Concurrency.Common;
using Concurrency.Common.State;
using System.Diagnostics;

namespace Replication.Implementation;

internal class HistoryNode
{
    readonly SnapperGrainID myGrainID;

    /// <summary> set true when the node is replicated </summary>
    public readonly bool isReplica;

    public readonly SnapperID id;

    /// <summary> the time when this state is captured </summary>
    public DateTime timestamp;

    public SnapperGrainState state;

    public SnapperGrainStateUpdates updates;

    public HistoryNode? prev { get; set; }

    public HistoryNode? next { get; set; }

    /// <summary> 
    /// for replicated ndoe: set true when all related grains have registered the replicated state 
    /// for read-only node: set true when all transactions in the node have completed
    /// </summary>
    public TaskCompletionSource waitForCommit { get; set; }

    /// <summary> the set of read-only ACTs in this node </summary>
    HashSet<SnapperID> acts;

    /// <summary> key is primary bid, the set of read-only PACTs in this node </summary>
    Dictionary<SnapperID, HashSet<SnapperID>> pactsPerBatch;

    public HistoryNode(SnapperGrainID myGrainID, SnapperID id, bool isReplica = true)
    {
        this.myGrainID = myGrainID;
        timestamp = DateTime.MinValue;
        state = new SnapperGrainState(myGrainID.grainID);
        updates = new SnapperGrainStateUpdates();
        prev = null;
        next = null;
        waitForCommit = new TaskCompletionSource();
        this.id = id;
        this.isReplica = isReplica;
        acts = new HashSet<SnapperID>();
        pactsPerBatch = new Dictionary<SnapperID, HashSet<SnapperID>>();
    }

    // =========================================================================== for replication
    public void SetState(DateTime timestamp, SnapperGrainStateUpdates updates)
    {
        this.timestamp = timestamp;
        this.updates = updates;
    }

    /// <summary> the head node will take state from another node </summary>
    public void MergeState(HistoryNode node)
    {
        timestamp = node.timestamp;
        Debug.Assert(node.state.dictionary.Count == 0 && node.state.references.Count == 0 && node.state.list.Count == 0);
        Debug.Assert(updates.updatesOnDictionary.Count == 0 && updates.updatesOnReference.Count == 0 && updates.updatesOnList.Count == 0);
        state.ApplyUpdates(node.updates);
    }

    // ===================================================== for read-only transactions on replica
    public void AddACT(SnapperID tid) => acts.Add(tid);

    public void AddPACTs(SnapperID primaryBid, List<SnapperID> tids)
    {
        Debug.Assert(!pactsPerBatch.ContainsKey(primaryBid));
        pactsPerBatch.Add(primaryBid, tids.ToHashSet());
    }

    public async Task WaitForTurn()
    {
        Debug.Assert(prev != null);
        Debug.Assert(!isReplica);

        // find the closest replica node
        var node = prev;
        if (!node.isReplica)
        {
            Debug.Assert(node.prev != null);
            Debug.Assert(node.prev.isReplica);
            node = node.prev;
        }
        Debug.Assert(node != null);

        // wait for the replica node to commit
        await node.waitForCommit.Task;
    }

    public SnapperGrainState GetState() => state;

    /// <returns> 
    /// if all read-only transactions in this node have completed 
    /// if the batch has all PACTs completed
    /// </returns>
    public (bool, bool) CompleteTxn(TransactionContext cxt)
    {
        // remove the PACT
        var batchComplete = false;
        if (cxt.isDet && pactsPerBatch.ContainsKey(cxt.bid) && pactsPerBatch[cxt.bid].Contains(cxt.tid))
        {
            pactsPerBatch[cxt.bid].Remove(cxt.tid);
            //Console.WriteLine($"Grain {Helper.ConvertGuidToInt(myGrainID.id)}: complete tid = {cxt.tid.id}, bid = {cxt.tid.id}");
            if (pactsPerBatch[cxt.bid].Count == 0)
            {
                pactsPerBatch.Remove(cxt.bid);
                batchComplete = true;
            }
        }

        // remove the ACT
        if (!cxt.isDet && acts.Contains(cxt.tid)) acts.Remove(cxt.tid);

        return (acts.Count == 0 && pactsPerBatch.Count == 0, batchComplete);
    }
}
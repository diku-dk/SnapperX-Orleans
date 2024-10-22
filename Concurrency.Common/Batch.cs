using Concurrency.Common.State;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common;

[GenerateSerializer]
public class Batch : ICloneable
{
    [Id(0)]
    public SnapperID global_bid;

    [Id(1)]
    public SnapperID global_prevBid;

    [Id(2)]
    public SnapperID regional_bid;

    [Id(3)]
    public SnapperID regional_prevBid;

    [Id(4)]
    public SnapperID local_bid;

    [Id(5)]
    public SnapperID local_prevBid;

    [Id(6)]
    public SnapperID local_prev_primaryBid;

    [Id(7)]
    public List<(SnapperID, ActorAccessInfo)> txnList;

    [Id(8)]
    public SnapperGrainID coordID;

    /// <summary> key, first tid in the corresponding txnList on the key, depBid, depTid </summary>
    [Id(9)]
    public Dictionary<ISnapperKey, Dictionary<SnapperID, (SnapperID, SnapperID)>> txnDependencyPerKey;

    public Batch()
    {
        global_bid = new SnapperID();
        global_prevBid = new SnapperID();
        regional_bid = new SnapperID();
        regional_prevBid = new SnapperID();
        local_bid = new SnapperID();
        local_prevBid = new SnapperID();
        local_prev_primaryBid = new SnapperID();
        txnList = new List<(SnapperID, ActorAccessInfo)>();
        coordID = new SnapperGrainID(Guid.Empty, "");
        txnDependencyPerKey = new Dictionary<ISnapperKey, Dictionary<SnapperID, (SnapperID, SnapperID)>>();
    }

    public void SetSnapperID(SnapperID bid, SnapperID prevBid, Hierarchy hierarchy)
    {
        switch (hierarchy)
        {
            case Hierarchy.Global:
                global_bid = bid;
                global_prevBid = prevBid;
                Debug.Assert(regional_bid.isEmpty());
                Debug.Assert(local_bid.isEmpty());
                break;
            case Hierarchy.Regional:
                regional_bid = bid;
                regional_prevBid = prevBid;
                Debug.Assert(local_bid.isEmpty());
                break;
            case Hierarchy.Local:
                local_bid = bid;
                local_prevBid = prevBid;
                break;
        }
    }

    public void SetPrevLocalPrimaryBid(SnapperID id)
    {
        if (local_prevBid.isEmpty()) Debug.Assert(id.isEmpty());
        else Debug.Assert(!id.isEmpty());

        local_prev_primaryBid = id;
    }

    public void AddTxn((SnapperID, ActorAccessInfo) txn) => txnList.Add(txn);

    public SnapperID GetPrimaryBid()
    {
        if (!global_bid.isEmpty()) return global_bid;
        else if (!regional_bid.isEmpty()) return regional_bid;
        else return local_bid;
    }

    public void AddTxnDependencyOnKey(ISnapperKey key, SnapperID tid, SnapperID depBid, SnapperID depTid)
    {
        if (!txnDependencyPerKey.ContainsKey(key)) txnDependencyPerKey.Add(key, new Dictionary<SnapperID, (SnapperID, SnapperID)>());
        txnDependencyPerKey[key].Add(tid, (depBid, depTid));
    }

    public (SnapperID, SnapperID) GetDependentTransactionOnKey(ISnapperKey key, SnapperID tid)
    {
        if (!txnDependencyPerKey.ContainsKey(key) || !txnDependencyPerKey[key].ContainsKey(tid)) return (new SnapperID(), new SnapperID());
        else return txnDependencyPerKey[key][tid];
    }
    
    public SnapperID GetDependentTransaction(SnapperID tid)
    {
        var index = 0;
        while (index < txnList.Count)
        {
            if (txnList[index].Item1.Equals(tid)) break;
            index++;
        }
        if (index != 0) return txnList[index - 1].Item1;
        else return new SnapperID();
    }

    public object Clone()
    {
        return new Batch
        {
            global_bid = (SnapperID)global_bid.Clone(),
            global_prevBid = (SnapperID)global_prevBid.Clone(),
            regional_bid = (SnapperID)regional_bid.Clone(),
            regional_prevBid = (SnapperID)regional_prevBid.Clone(),
            local_bid = (SnapperID)local_bid.Clone(),
            local_prevBid = (SnapperID)local_prevBid.Clone(),
            local_prev_primaryBid = (SnapperID)local_prev_primaryBid.Clone(),
            txnList = new List<(SnapperID, ActorAccessInfo)>(txnList),
            coordID = new SnapperGrainID(coordID.grainID.id, coordID.location)
        };
    }
}
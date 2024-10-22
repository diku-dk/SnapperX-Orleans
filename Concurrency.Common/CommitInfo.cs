using Utilities;

namespace Concurrency.Common;

[GenerateSerializer]
public class CommitInfo
{
    /// <summary> the highest committed global bid </summary>
    [Id(0)]
    public long globalBid;

    /// <summary> region ID, highest committed regional bid </summary>
    [Id(1)]
    public Dictionary<string, long> regionalBid;

    /// <summary> region ID, silo ID, highest committed local bid </summary>
    [Id(2)]
    public Dictionary<string, Dictionary<string, long>> localBid;

    public CommitInfo()
    {
        globalBid = -1;
        regionalBid = new Dictionary<string, long>();
        localBid = new Dictionary<string, Dictionary<string, long>>();
    }

    public void MergeCommitInfo(CommitInfo info)
    {
        globalBid = Math.Max(globalBid, info.globalBid);

        foreach (var item in info.regionalBid)
        {
            var regionID = item.Key;
            if (!regionalBid.ContainsKey(regionID)) regionalBid[regionID] = item.Value;
            else regionalBid[regionID] = Math.Max(item.Value, regionalBid[regionID]);
        };

        foreach (var itemx in info.localBid)
        {
            var regionID = itemx.Key;
            if (!localBid.ContainsKey(regionID)) localBid.Add(regionID, new Dictionary<string, long>());
            foreach (var itemy in itemx.Value)
            {
                var siloID = itemy.Key;
                if (!localBid[regionID].ContainsKey(siloID)) localBid[regionID][siloID] = itemy.Value;
                else localBid[regionID][siloID] = Math.Max(localBid[regionID][siloID], itemy.Value);
            }
        }
    }

    public bool isBatchCommit(SnapperID bid)
    {
        if (bid.isEmpty()) return true;

        switch (bid.hierarchy)
        {
            case Hierarchy.Local:
                if (!localBid.ContainsKey(bid.regionID) || !localBid[bid.regionID].ContainsKey(bid.siloID)) return false;
                else return localBid[bid.regionID][bid.siloID] >= bid.id;
            case Hierarchy.Regional:
                if (!regionalBid.ContainsKey(bid.regionID)) return false;
                else return regionalBid[bid.regionID] >= bid.id;
            case Hierarchy.Global:
                return globalBid >= bid.id;
            default: throw new Exception($"Unsupported hierarchy {bid.hierarchy}");
        }
    }

    public void CommitBatch(SnapperID bid)
    {
        switch (bid.hierarchy)
        {
            case Hierarchy.Local:
                if (!localBid.ContainsKey(bid.regionID)) localBid.Add(bid.regionID, new Dictionary<string, long>());
                if (!localBid[bid.regionID].ContainsKey(bid.siloID)) localBid[bid.regionID][bid.siloID] = bid.id;
                else localBid[bid.regionID][bid.siloID] = Math.Max(bid.id, localBid[bid.regionID][bid.siloID]);
                break;
            case Hierarchy.Regional:
                if (!regionalBid.ContainsKey(bid.regionID)) regionalBid[bid.regionID] = bid.id;
                else regionalBid[bid.regionID] = Math.Max(bid.id, regionalBid[bid.regionID]);
                break;
            case Hierarchy.Global:
                globalBid = Math.Max(bid.id, globalBid);
                break;
        }
    }
}
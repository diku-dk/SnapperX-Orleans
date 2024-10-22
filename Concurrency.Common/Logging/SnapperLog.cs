using MessagePack;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.Logging;

[GenerateSerializer]
[MessagePackObject]
public class SnapperLog
{
    [Key(0)]
    [Id(0)]
    public readonly SnapperLogType type;

    [Key(1)]
    [Id(1)]
    public readonly byte[] content;

    public SnapperLog(SnapperLogType type, byte[] content)
    {
        this.type = type;
        this.content = content;
    }
}

[MessagePackObject]
public class InfoLog
{
    /// <summary> it is either a bid or an ACT tid </summary>
    [Key(0)]
    public readonly SnapperID id;

    /// <summary> the set of grains that are updated by the batch or ACT </summary>
    [Key(1)]
    public readonly HashSet<GrainID> grains;

    public InfoLog(SnapperID id, HashSet<GrainID> grains)
    { 
        this.id = id;
        this.grains = grains;
    }
}

[GenerateSerializer]
[MessagePackObject]
public class PrepareLog
{
    [Key(0)]
    [Id(0)]
    public readonly GrainID grainID;

    [Key(1)]
    [Id(1)]
    public readonly SnapperID id;

    [Key(2)]
    [Id(2)]
    public readonly DateTime timestamp;

    [Key(3)]
    [Id(3)]
    public readonly SnapperID prevId;

    [Key(4)]
    [Id(4)]
    public readonly DateTime prevTimestamp;

    [Key(5)]
    [Id(5)]
    public readonly byte[] updatesOnDictionary;

    [Key(6)]
    [Id(6)]
    public readonly byte[] updatesOnReference;

    [Key(7)]
    [Id(7)]
    public readonly byte[] updatesOnList;

    public PrepareLog(GrainID grainID, SnapperID id, DateTime timestamp, SnapperID prevId, DateTime prevTimestamp, byte[] updatesOnDictionary, byte[] updatesOnReference, byte[] updatesOnList)
    {
        this.grainID = grainID;
        this.id = id;
        this.timestamp = timestamp;
        this.prevId = prevId;
        this.prevTimestamp = prevTimestamp;
        this.updatesOnDictionary = updatesOnDictionary;
        this.updatesOnReference = updatesOnReference;
        this.updatesOnList = updatesOnList;
        Debug.Assert(prevTimestamp < timestamp);
    }
}

[MessagePackObject]
public class CommitLog
{
    [Key(0)]
    public readonly SnapperID id;

    public CommitLog(SnapperID id) => this.id = id;
}
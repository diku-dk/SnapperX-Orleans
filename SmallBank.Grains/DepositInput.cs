using Concurrency.Common.State;
using MessagePack;
using SmallBank.Grains.State;
using Utilities;

namespace SmallBank.Grains;

[MessagePackObject]
[GenerateSerializer]
public class DepositInput
{
    /// <summary> userID, money </summary>
    [Id(0)]
    [Key(0)]
    public readonly Dictionary<UserID, int> writeInfo;

    [Id(1)]
    [Key(1)]
    public readonly HashSet<UserID> readInfo;

    public DepositInput()
    {
        writeInfo = new Dictionary<UserID, int>();
        readInfo = new HashSet<UserID>();
    }

    public DepositInput(Dictionary<UserID, int> writeInfo, HashSet<UserID> readInfo)
    {
        this.writeInfo = writeInfo; 
        this.readInfo = readInfo;
    }

    public Dictionary<ISnapperKey, AccessMode> GetKeyAccessMode()
    {
        var info = new Dictionary<ISnapperKey, AccessMode>();
        foreach (var item in writeInfo) info.Add(item.Key, AccessMode.ReadWrite);
        foreach (var item in readInfo) info.Add(item, AccessMode.Read);
        return info;
    }
}
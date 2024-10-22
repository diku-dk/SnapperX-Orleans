using Concurrency.Common;
using Concurrency.Common.State;
using MessagePack;
using SmallBank.Grains.State;
using System.Diagnostics;
using Utilities;

namespace SmallBank.Grains;

[MessagePackObject]
[GenerateSerializer]
public class MultiTransferInput
{
    /// <summary> fromUser => (toUser, money) </summary>
    [Id(0)]
    [Key(0)]
    public readonly Dictionary<UserID, Dictionary<UserID, int>> writeInfo;

    /// <summary> the set of keys  need to read </summary>
    [Id(1)]
    [Key(1)]
    public readonly HashSet<UserID> readInfo;

    public MultiTransferInput()
    {
        writeInfo = new Dictionary<UserID, Dictionary<UserID, int>>();
        readInfo = new HashSet<UserID>();
    }

    public MultiTransferInput(Dictionary<UserID, Dictionary<UserID, int>> writeInfo, HashSet<UserID> readInfo)
    {
        this.writeInfo = writeInfo;
        this.readInfo = readInfo;
    }

    public void Print(string grainName)
    {
        var info = GetKeyAccessInfo(grainName);

        foreach (var item in info)
            Console.WriteLine($"grainID = {item.Key.className}-{Helper.ConvertGuidToIntString(item.Key.id)}, #keys = {item.Value.Count}");
    }

    public Dictionary<GrainID, HashSet<ISnapperKey>> GetKeyAccessInfo(string grainName)
    {
        var info = new Dictionary<GrainID, HashSet<ISnapperKey>>();
        foreach (var item in writeInfo)
        {
            var fromUser = item.Key;
            var fromGrain = new GrainID(SmallBankIdMapping.GetGrainGuid(fromUser), grainName);
            if (!info.ContainsKey(fromGrain)) info.Add(fromGrain, new HashSet<ISnapperKey>());
            info[fromGrain].Add(fromUser);

            foreach (var iitem in item.Value)
            {
                var toUser = iitem.Key;
                var toGrain = new GrainID(SmallBankIdMapping.GetGrainGuid(toUser), grainName);
                if (!info.ContainsKey(toGrain)) info.Add(toGrain, new HashSet<ISnapperKey>());
                info[toGrain].Add(toUser);
            }
        }

        foreach (var userID in readInfo)
        {
            var grainID = new GrainID(SmallBankIdMapping.GetGrainGuid(userID), grainName);
            if (!info.ContainsKey(grainID)) info.Add(grainID, new HashSet<ISnapperKey>());
            info[grainID].Add(userID);
        }

        return info;
    }

    public Dictionary<GrainID, Dictionary<UserID, int>> GetWriteInfoPerGrain(string grainName)
    {
        var info = new Dictionary<GrainID, Dictionary<UserID, int>>();
        foreach (var item in writeInfo)
        {
            var moneyToWithdraw = 0;
            foreach (var iitem in item.Value)
            {
                var toUser = iitem.Key;
                var moneyToDeposit = iitem.Value;

                var toGrainID = new GrainID(SmallBankIdMapping.GetGrainGuid(toUser), grainName);
                if (!info.ContainsKey(toGrainID)) info.Add(toGrainID, new Dictionary<UserID, int>());

                info[toGrainID].Add(toUser, moneyToDeposit);
                moneyToWithdraw += moneyToDeposit;
            }

            var fromUser = item.Key;
            var fromGrainID = new GrainID(SmallBankIdMapping.GetGrainGuid(fromUser), grainName);
            if (!info.ContainsKey(fromGrainID)) info.Add(fromGrainID, new Dictionary<UserID, int>());
            info[fromGrainID].Add(fromUser, moneyToWithdraw);
        }

        if (info.Count != 0)
        {
            var count = info.First().Value.Count;
            foreach (var item in info) Debug.Assert(item.Value.Count == count);
        }

        return info;
    }

    public Dictionary<GrainID, HashSet<UserID>> GetReadInfoPerGrain(string grainName)
    {
        var info = new Dictionary<GrainID, HashSet<UserID>>();
        foreach (var userID in readInfo)
        {
            var grainID = new GrainID(SmallBankIdMapping.GetGrainGuid(userID), grainName);
            if (!info.ContainsKey(grainID)) info.Add(grainID, new HashSet<UserID>());
            info[grainID].Add(userID);
        }

        if (info.Count != 0)
        {
            var count = info.First().Value.Count;
            foreach (var item in info) Debug.Assert(item.Value.Count == count);
        }
        
        return info;
    }

    public static Dictionary<ISnapperKey, AccessMode> GetKeyAccessMode(GrainID grainID, Dictionary<GrainID, Dictionary<UserID, int>> writeInfo, Dictionary<GrainID, HashSet<UserID>> readInfo)
    {
        var writeKeys = writeInfo.ContainsKey(grainID) ? writeInfo[grainID] : new Dictionary<UserID, int>();
        var readKeys = readInfo.ContainsKey(grainID) ? readInfo[grainID] : new HashSet<UserID>();

        var info = new Dictionary<ISnapperKey, AccessMode>();
        foreach (var item in writeKeys) info.Add(item.Key, AccessMode.ReadWrite);
        foreach (var item in readKeys) info.Add(item, AccessMode.Read);
        return info;
    }
}
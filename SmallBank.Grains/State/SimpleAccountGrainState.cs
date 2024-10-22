using Concurrency.Common.State;
using MessagePack;
using System.Diagnostics;
using Utilities;

namespace SmallBank.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class SimpleAccountGrainState : AbstractValue<SimpleAccountGrainState>, ISnapperValue, IEquatable<SimpleAccountGrainState>
{
    [Key(0)]
    [Id(0)]
    public Dictionary<UserID, AccountInfo> users;

    /// <summary> origin key, follower key, reference type </summary>
    [Key(1)]
    [Id(1)]
    public Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>> followersInfo;

    /// <summary> follower key, origin key </summary>
    [Key(2)]
    [Id(2)]
    public Dictionary<UserID, UserID> originInfo;

    public SimpleAccountGrainState()
    {
        users = new Dictionary<UserID, AccountInfo>();
        followersInfo = new Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>();
        originInfo = new Dictionary<UserID, UserID>();
    }

    public SimpleAccountGrainState(
        Dictionary<UserID, AccountInfo> users, 
        Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>> followersInfo,
        Dictionary<UserID, UserID> originInfo)
    {
        this.users = users;
        this.followersInfo = followersInfo;
        this.originInfo = originInfo;
    }

    public object Clone()
    {
        var usersCopy = Helper.CloneDictionary(users);
        Debug.Assert(Helper.CheckDictionaryEqualty(usersCopy, users));

        var followersInfoCopy = new Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>();
        foreach (var item in followersInfo)
        {
            var originKey = (UserID)item.Key.Clone();
            followersInfoCopy.Add(originKey, new Dictionary<UserID, SnapperKeyReferenceType>());

            foreach (var iitem in item.Value)
                followersInfoCopy[originKey].Add((UserID)iitem.Key.Clone(), iitem.Value);
        }

        var originInfoCopy = Helper.CloneDictionary(originInfo);
        Debug.Assert(Helper.CheckDictionaryEqualty(originInfoCopy, originInfo));

        return new SimpleAccountGrainState(usersCopy, followersInfoCopy, originInfoCopy);
    }

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as SimpleAccountGrainState;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(SimpleAccountGrainState? data)
    {
        if (data == null) return false;
        if (!Helper.CheckDictionaryEqualty(users, data.users)) return false;

        if (followersInfo.Count != data.followersInfo.Count) return false;
        foreach (var item in data.followersInfo)
        {
            if (!followersInfo.ContainsKey(item.Key)) return false;
            foreach (var iitem in item.Value)
            {
                if (!followersInfo[item.Key].ContainsKey(iitem.Key)) return false;
                if (!followersInfo[item.Key][iitem.Key].Equals(iitem.Value)) return false;
            }
        }

        if (!Helper.CheckDictionaryEqualty(originInfo, data.originInfo)) return false;

        return true;
    }

    public string Print() => "SimpleAccountGrainState";
}
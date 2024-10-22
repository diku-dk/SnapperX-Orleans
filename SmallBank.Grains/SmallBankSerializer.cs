using Concurrency.Common;
using MessagePack;
using SmallBank.Grains.State;
using System.Diagnostics;
using Utilities;

namespace SmallBank.Grains;

public static class SmallBankSerializer
{
    public static (byte[], byte[], byte[], byte[]) SerializeAccountGrainState(Dictionary<UserID, AccountInfo> users, Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>> followersInfo, Dictionary<UserID, UserID> originInfo)
    {
        var item1 = new List<(byte[], byte[])>();
        foreach (var item in users) item1.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        var item2 = new List<(byte[], byte[], byte[])>();
        foreach (var item in followersInfo)
        {
            var originKey = MessagePackSerializer.Serialize(item.Key);
            foreach (var iitem in item.Value)
            {
                var followerKey = MessagePackSerializer.Serialize(iitem.Key);
                var referenceType = MessagePackSerializer.Serialize(iitem.Value);
                item2.Add((originKey, followerKey, referenceType));
            }
        }
        
        var item3 = new List<(byte[], byte[])>();
        foreach (var item in originInfo) item3.Add((MessagePackSerializer.Serialize(item.Key), MessagePackSerializer.Serialize(item.Value)));

        return (MessagePackSerializer.Serialize(item1), MessagePackSerializer.Serialize(item2), MessagePackSerializer.Serialize(item3), new byte[0]);
    }

    public static (Dictionary<UserID, AccountInfo>, Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>, Dictionary<UserID, UserID>) DeSerializeAccountGrainState((byte[], byte[], byte[], byte[]) bytes)
    {
        var users = new Dictionary<UserID, AccountInfo>();
        var item1 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item1);
        foreach (var item in item1) users.Add(MessagePackSerializer.Deserialize<UserID>(item.Item1), MessagePackSerializer.Deserialize<AccountInfo>(item.Item2));

        var followersInfo = new Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>();
        var item2 = MessagePackSerializer.Deserialize<List<(byte[], byte[], byte[])>>(bytes.Item2);
        foreach (var item in item2)
        {
            var originKey = MessagePackSerializer.Deserialize<UserID>(item.Item1);
            var followerKey = MessagePackSerializer.Deserialize<UserID>(item.Item2);
            var referenceType = MessagePackSerializer.Deserialize<SnapperKeyReferenceType>(item.Item3);

            if (!followersInfo.ContainsKey(originKey)) followersInfo.Add(originKey, new Dictionary<UserID, SnapperKeyReferenceType>());
            if (!followersInfo[originKey].ContainsKey(followerKey)) followersInfo[originKey].Add(followerKey, referenceType);
        }

        var originInfo = new Dictionary<UserID, UserID>();
        var item3 = MessagePackSerializer.Deserialize<List<(byte[], byte[])>>(bytes.Item3);
        foreach (var item in item3) originInfo.Add(MessagePackSerializer.Deserialize<UserID>(item.Item1), MessagePackSerializer.Deserialize<UserID>(item.Item2));

        return (users, followersInfo, originInfo);
    }

    public static void CheckStateCorrectnessForOrleans(Dictionary<GrainID, (byte[], byte[], byte[], byte[])> masterGrains)
    {
        var users = new Dictionary<GrainID, Dictionary<UserID, AccountInfo>>();
        var followersInfo = new Dictionary<GrainID, Dictionary<UserID, Dictionary<UserID, SnapperKeyReferenceType>>>();
        var originInfo = new Dictionary<GrainID, Dictionary<UserID, UserID>>();

        foreach (var item in masterGrains)
        {
            var grainID = item.Key;

            (var uss, var fol, var ori) = DeSerializeAccountGrainState(item.Value);
            users.Add(grainID, uss);
            followersInfo.Add(grainID, fol);
            originInfo.Add(grainID, ori);
        }

        // check if the follower keys are consistent with the original version
        var numOriginKey = 0;
        var numFollowerKey = 0;
        foreach (var item in followersInfo)
        {
            var originActorID = item.Key;

            foreach (var dependency in item.Value)
            {
                numOriginKey++;
                var originKey = dependency.Key;

                Debug.Assert(users.ContainsKey(originActorID));
                Debug.Assert(users[originActorID].ContainsKey(originKey));
                var originValue = users[originActorID][originKey];
                Debug.Assert(originValue != null);

                foreach (var info in dependency.Value)
                {
                    numFollowerKey++;
                    var followerKey = info.Key;
                    var referenceType = info.Value;

                    var followerActorID = new GrainID(Helper.ConvertIntToGuid(followerKey.actorID), originActorID.className);

                    // the follower key must exist
                    Debug.Assert(users.ContainsKey(followerActorID));
                    Debug.Assert(users[followerActorID].ContainsKey(followerKey));

                    // the follower value must be the same as the origin
                    if (referenceType == SnapperKeyReferenceType.ReplicateReference)
                    {
                        var followerValue = users[followerActorID][followerKey];
                        Debug.Assert(followerValue.Equals(originValue));
                    }

                    // the follower key must have its origin info registered
                    Debug.Assert(originInfo.ContainsKey(followerActorID));
                    Debug.Assert(originInfo[followerActorID].ContainsKey(followerKey));
                    Debug.Assert(originInfo[followerActorID][followerKey].Equals(originKey));
                }
            }
        }

        Console.WriteLine($"CheckDataConsistency: {numFollowerKey} follower keys are consistent with {numOriginKey} origin keys. ");
    }
}
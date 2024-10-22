using Concurrency.Common.State;
using MessagePack;

namespace SmallBank.Grains.State;

[GenerateSerializer]
[MessagePackObject]
public class UserID : AbstractKey<UserID>, ISnapperKey, IEquatable<UserID>
{
    [Key(0)]
    [Id(0)]
    public readonly int actorID;

    [Key(1)]
    [Id(1)]
    public readonly int userID;

    public UserID(int actorID, int userID) { this.actorID = actorID; this.userID = userID; }

    public UserID() { actorID = -1; userID = -1; }

    public object Clone() => new UserID(actorID, userID);

    public bool Equals(ISnapperKey? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as UserID;
            return Equals(data);
        }
        catch { return false; }
    }

    public bool Equals(UserID? data)
    {
        if (data == null) return false;
        return actorID == data.actorID && userID == data.userID;
    }

    public override int GetHashCode() => actorID.GetHashCode() ^ userID.GetHashCode();

    public string Print() => $"{actorID}-{userID}";
}
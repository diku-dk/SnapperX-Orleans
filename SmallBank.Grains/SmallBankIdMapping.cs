using Concurrency.Common;
using SmallBank.Grains.State;
using Utilities;

namespace SmallBank.Grains;

public static class SmallBankIdMapping
{
    public static Guid GetGrainGuid(UserID userID) => Helper.ConvertIntToGuid(userID.actorID);
    
    public static UserID GetUserID(GrainID grainID, int id)
    {
        var actorID = Helper.ConvertGuidToInt(grainID.id);
        var userID = new UserID(actorID, id);
        return userID;
    }
}
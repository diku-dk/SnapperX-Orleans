using Concurrency.Common.ICache;

namespace Concurrency.Common;

public static class GrainReferenceHelper
{
    /// <summary> get the reference of a grain with the given ID </summary>
    public static T GetGrainReference<T>(ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory, string myRegionID, GrainID actorID) 
        where T : IGrainWithGuidCompoundKey
    {
        var masterGrainID = snapperClusterCache.GetMasterGrain(actorID);

        T actor;
        (var regionID, _) = masterGrainID.GetLocation();
        if (regionID != myRegionID)
        {
            var client = snapperClusterCache.GetOrleansClient(regionID);
            actor = client.GetGrain<T>(masterGrainID.grainID.id, masterGrainID.location);
        }
        else actor = grainFactory.GetGrain<T>(masterGrainID.grainID.id, masterGrainID.location);

        return actor;
    }
}
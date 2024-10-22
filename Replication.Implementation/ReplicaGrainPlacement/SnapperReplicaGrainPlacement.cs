using Orleans.Runtime;
using Orleans.Placement;
using Orleans.Runtime.Placement;
using Utilities;

namespace Replication.Implementation.GrainReplicaPlacement;

public class SnapperReplicaGrainPlacement : IPlacementDirector
{
    public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
    {
        var silos = context.GetCompatibleSilos(target).ToList();

        if (silos.Count == 1) return Task.FromResult(silos[0]);

        // example GrainIdentity = snappertransactionalaccount / 00000000000000000000000000000970 + regionID + siloID
        //                         globalcoord                 / 00000000000000000000000000000000 + regionID + siloID
        //                         regionalcoord               / 00000000000000000000000000000000 + regionID + siloID
        //                         localcoord                  / 00000000000000000000000000000003 + regionID + siloID
        var strs = target.GrainIdentity.ToString().Split('+');
        
        var grainName = strs[0].Split('/')[0];
        var grainGuid = target.GrainIdentity.GetGuidKey();
        var regionID = strs[1];
        var siloID = strs[2];

        foreach (var silo in silos)
        {
            var siloString = Helper.SiloStringToRuntimeID(silo.ToParsableString());
            if (siloString.Equals(siloID))
            {
                //Console.WriteLine($"Placement: {grainName}, {regionID}, {siloID}, select silo = {silo.ToParsableString()}, siloString = {siloString}");
                return Task.FromResult(silo);
            } 
        }
        throw new Exception($"Fail to find silo {siloID}");

    }
}

[GenerateSerializer]
public class SnapperReplicaGrainPlacementStrategy : PlacementStrategy
{
    public SnapperReplicaGrainPlacementStrategy() { }
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public sealed class SnapperReplicaGrainPlacementStrategyAttribute : PlacementAttribute
{
    public SnapperReplicaGrainPlacementStrategyAttribute() : base(new SnapperReplicaGrainPlacementStrategy()) { }
}
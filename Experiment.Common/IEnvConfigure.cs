using Concurrency.Common;
using Concurrency.Common.Cache;
using Concurrency.Common.ICache;
using Utilities;

namespace Experiment.Common;

public interface IEnvConfigure
{
    // ============================================================= for transactional silo ===================
    public void GenerateGrainPlacementInfo(List<string> regionList, IEnvSetting iEnvSetting, StaticClusterInfo staticClusterInfo);

    public (Dictionary<GrainID, int>, Dictionary<int, (string, string)>, Dictionary<GrainID, (Guid, string)>) GetGrainPlacementInfo();

    public Task InitAllGrains(string myRegion, string mySilo, IEnvSetting iEnvSetting, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory);

    public Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperClusterCache snapperClusterCache, IGrainFactory grainFactory);

    public Task<AggregatedExperimentData> CheckGC(string myRegion, string mySilo, ImplementationType implementationType, IGrainFactory grainFactory);

    // ============================================================= for replica silo =========================

    public void GenerateReplicaGrainPlacementInfo(List<string> regionList, IEnvSetting iEnvSetting, StaticReplicaInfo staticReplicaInfo);

    public (Dictionary<GrainID, int>, Dictionary<string, Dictionary<int, string>>) GetReplicaGrainPlacementInfo();

    public Task InitAllReplicaGrains(string myRegion, string mySilo, IGrainFactory grainFactory);

    public Task<Dictionary<GrainID, (byte[], byte[], byte[], byte[])>> GetAllReplicaGrainState(string myRegion, string mySilo, ImplementationType implementationType, ISnapperReplicaCache snapperReplicaCache, IGrainFactory grainFactory);

    public Task CheckGCForReplica(string myRegion, string mySilo, IGrainFactory grainFactory);
}
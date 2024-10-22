using Concurrency.Common;
using Experiment.Common;
using System.Diagnostics;
using TestApp.Grains;
using Utilities;

namespace TestApp.Workload;

public class GrainNameHelper : IGrainNameHelper
{
    static string snapper_transactional = typeof(SnapperTransactionalTestGrain).FullName;
    static string snapper_replicated = typeof(SnapperReplicatedTestGrain).FullName;

    static Dictionary<string, string> nameMapping = new Dictionary<string, string>
    {
        { snapper_replicated, snapper_transactional }
    };

    public string GetGrainClassName(ImplementationType implementationType, bool isReplica)
    {
        var name = "";

        switch (implementationType)
        {
            case ImplementationType.SNAPPER:
                if (!isReplica) name = snapper_transactional;
                else name = snapper_replicated;
                break;
            default:
                throw new Exception($"Fail to get grain class name for {BenchmarkType.TESTAPP} benchmark on {implementationType}. ");
        }

        Debug.Assert(!string.IsNullOrEmpty(name));
        return name;
    }

    public GrainID GetMasterGrainID(ImplementationType implementationType, GrainID replicaGrainID)
    {
        if (!nameMapping.ContainsKey(replicaGrainID.className)) throw new Exception($"Fail to find the master grain name for {replicaGrainID.className}");

        var guid = replicaGrainID.id;
        var name = nameMapping[replicaGrainID.className];
        return new GrainID(guid, name);
    }
}
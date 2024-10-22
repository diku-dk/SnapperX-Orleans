using Concurrency.Common;
using Utilities;

namespace Experiment.Common;

public interface IGrainNameHelper
{
    public GrainID GetMasterGrainID(ImplementationType implementationType, GrainID replicaGrainID);
}
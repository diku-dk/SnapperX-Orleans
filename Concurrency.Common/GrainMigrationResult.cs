using System.Diagnostics;
using Utilities;

namespace Concurrency.Common;

[GenerateSerializer]
public class GrainMigrationResult
{
    [Id(0)]
    public Dictionary<GrainMigrationTimeInterval, long> intervalTime;

    public GrainMigrationResult() => intervalTime = new Dictionary<GrainMigrationTimeInterval, long>();

    public void SetTime(GrainMigrationTimeInterval interval, long time)
    {
        Debug.Assert(!intervalTime.ContainsKey(interval));
        intervalTime.Add(interval, time);
    }
}
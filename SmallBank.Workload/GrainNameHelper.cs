using Concurrency.Common;
using Experiment.Common;
using SmallBank.Grains;
using System.Diagnostics;
using System.Reflection;
using Utilities;

namespace SmallBank.Workload;

public class GrainNameHelper : IGrainNameHelper
{
    static Dictionary<ImplementationType, (string, string)> nameMapping = new Dictionary<ImplementationType, (string, string)>
    {
        { ImplementationType.NONTXN,        ("",                                                     typeof(NonTransactionalSimpleAccountGrain).FullName )},
        { ImplementationType.NONTXNKV,      ("",                                                     typeof(NonTransactionalKeyValueAccountGrain).FullName )},
        { ImplementationType.ORLEANSTXN,    ("",                                                     typeof(OrleansTransactionalAccountGrain).FullName )},
        { ImplementationType.SNAPPERSIMPLE, ("",                                                     typeof(SnapperTransactionalSimpleAccountGrain).FullName )},
        { ImplementationType.SNAPPER,       (typeof(SnapperReplicatedKeyValueAccountGrain).FullName, typeof(SnapperTransactionalKeyValueAccountGrain).FullName )},
        { ImplementationType.SNAPPERFINE,   ("",                                                     typeof(SnapperTransactionalFineAccountGrain).FullName )}
    };

    static Dictionary<ImplementationType, Dictionary<string, Type>> typeMapping = new Dictionary<ImplementationType, Dictionary<string, Type>>
    {
        { ImplementationType.NONTXN,        new Dictionary<string, Type> { { typeof(NonTransactionalSimpleAccountGrain).FullName,       typeof(NonTransactionalSimpleAccountGrain) } } },
        { ImplementationType.NONTXNKV,      new Dictionary<string, Type> { { typeof(NonTransactionalKeyValueAccountGrain).FullName,     typeof(NonTransactionalKeyValueAccountGrain) } } },
        { ImplementationType.ORLEANSTXN,    new Dictionary<string, Type> { { typeof(OrleansTransactionalAccountGrain).FullName,         typeof(OrleansTransactionalAccountGrain) } } },
        { ImplementationType.SNAPPERSIMPLE, new Dictionary<string, Type> { { typeof(SnapperTransactionalSimpleAccountGrain).FullName,   typeof(SnapperTransactionalSimpleAccountGrain) } } },
        { ImplementationType.SNAPPER,       new Dictionary<string, Type> 
            { 
                { typeof(SnapperTransactionalKeyValueAccountGrain).FullName, typeof(SnapperTransactionalKeyValueAccountGrain) },
                { typeof(SnapperReplicatedKeyValueAccountGrain).FullName,    typeof(SnapperReplicatedKeyValueAccountGrain) }
            } 
        },
        { ImplementationType.SNAPPERFINE,   new Dictionary<string, Type> { { typeof(SnapperTransactionalFineAccountGrain).FullName,     typeof(SnapperTransactionalFineAccountGrain) } } }
    };

    static Dictionary<ImplementationType, Dictionary<string, string>> nameMap = new Dictionary<ImplementationType, Dictionary<string, string>> 
    {
        { ImplementationType.NONTXN,        new Dictionary<string, string>{ { "AccountGrain", typeof(NonTransactionalSimpleAccountGrain).FullName } } },
        { ImplementationType.NONTXNKV,      new Dictionary<string, string>{ { "AccountGrain", typeof(NonTransactionalKeyValueAccountGrain).FullName } } },
        { ImplementationType.ORLEANSTXN,    new Dictionary<string, string>{ { "AccountGrain", typeof(OrleansTransactionalAccountGrain).FullName } } },
        { ImplementationType.SNAPPERSIMPLE, new Dictionary<string, string>{ { "AccountGrain", typeof(SnapperTransactionalSimpleAccountGrain).FullName } } },
        { ImplementationType.SNAPPER,       new Dictionary<string, string>{ { "AccountGrain", typeof(SnapperTransactionalKeyValueAccountGrain).FullName } } },
        { ImplementationType.SNAPPERFINE,   new Dictionary<string, string>{ { "AccountGrain", typeof(SnapperTransactionalFineAccountGrain).FullName } } },
    };

    public static MethodInfo GetMethod(ImplementationType implementationType, string grainClassName, string funcName)
    {
        var snapperMethod = typeMapping[implementationType][grainClassName].GetMethod(funcName);
        if (snapperMethod == null) Console.WriteLine($"Fail to find method {funcName} on class {grainClassName}");
        Debug.Assert(snapperMethod != null);
        return snapperMethod;
    }

    public string GetGrainClassName(ImplementationType implementationType, bool isReplica)
    {
        Debug.Assert(nameMapping.ContainsKey(implementationType));
        if (isReplica)
        {
            Debug.Assert(!string.IsNullOrEmpty(nameMapping[implementationType].Item1));
            return nameMapping[implementationType].Item1;
        } 
        else return nameMapping[implementationType].Item2;
    }

    public GrainID GetMasterGrainID(ImplementationType implementationType, GrainID replicaGrainID)
    {
        (var replicaName, var masterName) = nameMapping[implementationType];
        Debug.Assert(replicaGrainID.className.Equals(replicaName));

        return new GrainID(replicaGrainID.id, masterName);
    }

    public Dictionary<string, string> GetNameMap(ImplementationType implementationType) => nameMap[implementationType];
}
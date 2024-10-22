using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Amazon;

namespace Utilities;

public static class Helper
{
    public static RegionEndpoint ParseRegionEndpoint(string regionID)
    {
        switch (regionID)
        {
            case "eu-north-1": return RegionEndpoint.EUNorth1;
            case "eu-west-3": return RegionEndpoint.EUWest3;
            default: throw new Exception($"Unsupported RegionEndpoint {regionID}.");
        }
    }

    public static string GetMembershipTableName(string regionID) => Constants.SiloMembershipTable + "-" + regionID;

    public static int CompareHierarchy(Hierarchy a, Hierarchy b)
    {
        if (a == b) return 0;
        else if (a == Hierarchy.Global) return 1;     // a > b
        else if (b == Hierarchy.Global) return -1;    // a < b
        else if (a == Hierarchy.Regional) return 1;   // a > b
        else if (b == Hierarchy.Regional) return -1;  // a < b
        return 0;
    }

    public static Hierarchy GetHigherHierarchy(Hierarchy hierarchy)
    {
        if (hierarchy == Hierarchy.Local) return Hierarchy.Regional;
        else if (hierarchy == Hierarchy.Regional) return Hierarchy.Global;
        else throw new Exception($"Fail to get a higher hierarchy for {hierarchy}");
    }

    public static Dictionary<SnapperRoleType, int> CalculateRoleSizes(int numRegion, int numSiloPerRegion)
    {
        var roleSizes = new Dictionary<SnapperRoleType, int>
        {
            { SnapperRoleType.Controller, 2 },
            { SnapperRoleType.Worker, Constants.numCPUPerLocalSilo },
            { SnapperRoleType.ReplicaWorker, Constants.numCPUPerLocalSilo },
            { SnapperRoleType.GlobalSilo, numRegion * numSiloPerRegion },
            { SnapperRoleType.RegionalSilo, numSiloPerRegion },
            { SnapperRoleType.LocalSilo, Constants.numCPUPerLocalSilo },
            { SnapperRoleType.LocalReplicaSilo, Constants.numCPUPerLocalSilo }
        };
        return roleSizes;
    }

    public static int GetLocalSiloIndex(int numSiloPerRegion, int regionIndex, int siloIndex) 
        => regionIndex * numSiloPerRegion + siloIndex;

    public static List<Guid> GetMasterGrainsOfPartition(int partitionID, int numGrainPerPartition)
    {
        var firstGrainID = partitionID * numGrainPerPartition;
        var grains = new List<Guid>();
        for (var i = 0; i < numGrainPerPartition; i++)
            grains.Add(ConvertIntToGuid(i + firstGrainID));
        return grains;
    }

    /// <summary> return (regionIndex, siloIndex) </summary>
    public static (int, int) GetPartitionMasterInfo(int partitionID, int numSiloPerRegion, int numMasterPartitionPerLocalSilo)
    {
        var regionIndex = partitionID / (numMasterPartitionPerLocalSilo * numSiloPerRegion);
        var siloIndex = (partitionID % (numMasterPartitionPerLocalSilo * numSiloPerRegion)) / numMasterPartitionPerLocalSilo;
        return (regionIndex, siloIndex);
    }

    public static string GetReplicaClassName(string className) => className.Replace("Transactional", "Replicated");

    public static int MapGuidToServiceID(Guid guid, int numService)
    {
        (var i1, var i2) = ConvertGuidToTwoInts(guid);
        if (i1 == 0) return i2 % numService;
        else return i1 % numService;
    }

    public static void SetCPU(string processName, int numCPU)
    {
        Console.WriteLine($"Set processor affinity for {processName}, numCPU = {numCPU}");
        var processes = Process.GetProcessesByName(processName);
        Debug.Assert(processes.Length == 1);

        var str = GetProcessorAffinityString(numCPU);
        var serverProcessorAffinity = Convert.ToInt64(str, 2);     // server uses the highest n bits

        processes[0].ProcessorAffinity = (IntPtr)serverProcessorAffinity;
    }

    static string GetProcessorAffinityString(int numCPU)
    {
        var str = "";
        Debug.Assert(numCPU <= Environment.ProcessorCount);

        for (int i = 0; i < Environment.ProcessorCount; i++)
        {
            if (i < numCPU) str += "1";
            else str += "0";
        }
        return str;
    }

    public static string GetLocalIPAddress()
    {
        var host = Dns.GetHostEntry(Dns.GetHostName());
        foreach (var ip in host.AddressList)
            if (ip.AddressFamily == AddressFamily.InterNetwork) return ip.ToString();

        throw new Exception("No network adapters with an IPv4 address in the system!");
    }

    public static string GetPublicIPAddress() => new WebClient().DownloadString("https://ipv4.icanhazip.com/").TrimEnd();
    
    public static string ChangeFormat(double n, int num)
    {
        if (double.IsNaN(n)) return "NaN";
        return Math.Round(n, num).ToString().Replace(',', '.');
    }

    public static Guid ConvertIntToGuid(int id1, int id2)
    {
        var str1 = id1.ToString().PadLeft(16, '0');
        var str2 = id2.ToString().PadLeft(16, '0');
        return new Guid(str1 + str2);
    }

    public static (int, int) ConvertGuidToTwoInts(Guid id)
    {
        var str = id.ToString("N");    // get a 32-digit string
        Debug.Assert(str.Length == 32);

        return (int.Parse(str.Substring(0, 16)), int.Parse(str.Substring(16, 16)));
    }

    public static string ConvertGuidToIntString(Guid id)
    {
        var str = id.ToString("N");    // get a 32-digit string
        Debug.Assert(str.Length == 32);

        var index = 0;
        while (index < str.Length && str[index] == '0') index++;
        if (index == str.Length) return "0";

        else
        {
            if (index >= 16)   // the guid is formed with only one integer
            {
                var number = int.Parse(str.Substring(index));
                Debug.Assert(number >= 0);
                return number.ToString();
            }
            else              // the guid is formed with two integers
            {
                var str1 = str.Substring(index, 16 - index);
                var number1 = int.Parse(str1);
                Debug.Assert(number1 >= 0);

                var newIndex = 16;
                while (newIndex < str.Length && str[index] == '0') newIndex++;
                if (newIndex == str.Length) return $"({number1}, 0)";
                else
                {
                    var number2 = int.Parse(str.Substring(newIndex));
                    Debug.Assert(number2 >= 0);
                    return $"({number1}, {number2})";
                }
            }
        }
    }

    public static Guid ConvertIntToGuid(int id) => new Guid(id.ToString().PadLeft(32, '0'));
    
    public static int ConvertGuidToInt(Guid id)
    {
        var str = id.ToString("N");
        var index = 0;
        while (index < str.Length && str[index] == '0') index++;
        if (index == str.Length) return 0;
        else return int.Parse(str.Substring(index));
    }

    public static string SiloStringToRuntimeID(string silo) => "S" + silo.Replace("@", ":");

    public static string ReFormSiloID(string siloID) => siloID.Replace(".", "-").Replace(":", "-");

    public static bool CheckDictionaryEqualty<T1, T2>(Dictionary<T1, T2> dictionary1, Dictionary<T1, T2> dictionary2)
        where T1 : IEquatable<T1>
        where T2 : IEquatable<T2>
    {
        if (dictionary1.Count != dictionary2.Count) return false;

        foreach (var item in dictionary1)
        {
            if (!dictionary2.ContainsKey(item.Key)) return false;
            if (!dictionary2[item.Key].Equals(item.Value)) return false;
        }

        return true;
    }

    public static Dictionary<T1, T2> CloneDictionary<T1, T2>(Dictionary<T1, T2> dictionary)
        where T1 : IEquatable<T1>, ICloneable
        where T2 : ICloneable
    {
        var newDictionary = new Dictionary<T1, T2>();
        foreach (var item in dictionary) newDictionary.Add((T1)item.Key.Clone(), (T2)item.Value.Clone());

        return newDictionary;
    }

    public static int GetACTPipeSize(ImplementationType implementationType, int totalNumGrain, int totalNumKey, int originalPipe)
    {
        switch (implementationType)
        {
            case ImplementationType.ORLEANSTXN:
                if (Constants.noDeadlock)
                {
                    switch (totalNumGrain)
                    {
                        case 10: return 2;
                        case 50: return 4;
                        case 100: return 4;
                        case 1000: return 16;
                        default: throw new Exception($"The totalNumGrain = {totalNumGrain}, is not supported. ");
                    }
                }
                else return originalPipe;
            case ImplementationType.SNAPPERSIMPLE:
            case ImplementationType.SNAPPER:
                switch (totalNumGrain)
                {
                    case 10: return 2;
                    case 50: return 4;
                    case 100: return 4;
                    case 1000: return 16;
                    default: throw new Exception($"The totalNumGrain = {totalNumGrain}, is not supported. ");
                }
            case ImplementationType.SNAPPERFINE:
                switch (totalNumKey)
                {
                    case 100: return 4;
                    case 500: return 8;
                    case 1000: return 16;
                    case 5000: return 64;
                    case 10000: return 128;
                    case 50000: return 512;
                    case 100000: return 512;
                    case 1000000: return 512;
                    default: throw new Exception($"The totalNumKey = {totalNumKey}, is not supported. ");
                }
            default:
                return originalPipe;
        }
    }
}
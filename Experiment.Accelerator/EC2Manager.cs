using Amazon.EC2;
using Amazon.EC2.Model;
using System.Diagnostics;
using Utilities;

namespace Experiment.Accelerator;

internal class EC2Manager
{
    readonly AmazonEC2Client client;
    List<string> instanceIDs;

    public EC2Manager(string AccessKey, string SecretKey)
    {
        client = new AmazonEC2Client(AccessKey, SecretKey, Amazon.RegionEndpoint.EUNorth1);
        instanceIDs = new List<string>();
    }

    public async Task CleanUp()
    {
        // retrieve and terminate all instances put in the "snapper" security group
        var instanceIDsToTerminate = new List<string>();
        foreach (var reservation in (await client.DescribeInstancesAsync()).Reservations)
        {
            foreach (var instance in reservation.Instances)
            {
                if (instance.InstanceId == Constants.localSnapperAmiInstanceID) continue;    // the ami inatance should not be deleted
                if (instance.Placement.GroupName == "snapper" && instance.State.Name != "terminated")
                {
                    instanceIDsToTerminate.Add(instance.InstanceId);
                    Console.WriteLine($"EC2Manager: The instance (id = {instance.InstanceId}, state = {instance.State.Name}) will be terminated");
                } 
            }
        }

        if (instanceIDsToTerminate.Count == 0) return;
        await client.TerminateInstancesAsync(new TerminateInstancesRequest { InstanceIds = instanceIDsToTerminate });
    }

    public async Task<SnapperServices> StartAllInstances(string password, int maxNumRegion, int maxNumLocalSilo, int maxNumLocalReplicaSilo)
    {
        Debug.Assert(maxNumRegion == 1);

        // read instance ids from file
        Debug.Assert(File.Exists(Constants.localInstanceInfo));

        instanceIDs.Clear();
        var numLocalSiloInstance = 0;
        var numWorkerInstance = 0;
        var numLocalReplicaSiloInstance = 0;
        var numReplicaWorkerInstance = 0;
        var instanceIDToType = new Dictionary<string, SnapperRoleType>();
        using (var file = new StreamReader(Constants.localInstanceInfo))
        {
            var line = file.ReadLine();
            while (line != null)
            {
                var strs = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                Debug.Assert(strs.Length == 2);
                var type = Enum.Parse<SnapperRoleType>(strs[0]);

                switch (type)
                {
                    case SnapperRoleType.Worker:
                        if (numWorkerInstance < maxNumRegion * maxNumLocalSilo) numWorkerInstance++;
                        else
                        {
                            line = file.ReadLine();
                            continue;
                        }
                        break;
                    case SnapperRoleType.ReplicaWorker:
                        if (numReplicaWorkerInstance < maxNumRegion * maxNumLocalReplicaSilo) numReplicaWorkerInstance++;
                        else
                        {
                            line = file.ReadLine();
                            continue;
                        }
                        break;
                    case SnapperRoleType.LocalSilo:
                        if (numLocalSiloInstance < maxNumRegion * maxNumLocalSilo) numLocalSiloInstance++;
                        else
                        {
                            line = file.ReadLine();
                            continue;
                        }
                        break;
                    case SnapperRoleType.LocalReplicaSilo:
                        if (numLocalReplicaSiloInstance < maxNumRegion * maxNumLocalReplicaSilo) numLocalReplicaSiloInstance++;
                        else
                        {
                            line = file.ReadLine();
                            continue;
                        }
                        break;
                }
                 
                instanceIDToType.Add(strs[1], type);
                instanceIDs.Add(strs[1]);
                line = file.ReadLine();
            }
        }

        // Start all instances
        await client.StartInstancesAsync(new StartInstancesRequest { InstanceIds = instanceIDs });

        // get the public ip of all instances
        var snapper = new SnapperServices();
        foreach (var instanceInfo in instanceIDToType)
        {
            var ip = "";
            InstanceStateName? state = null;
            // wait until the public ip is available and the instance is in the "running" state
            while (ip.Length == 0 || state == null)
            {
                var reservations = (await client.DescribeInstancesAsync(new DescribeInstancesRequest { InstanceIds = new List<string> { instanceInfo.Key } })).Reservations;
                Debug.Assert(reservations.Count == 1);
                Debug.Assert(reservations.First().Instances.Count == 1);
                Debug.Assert(reservations.First().Instances.First().InstanceId == instanceInfo.Key);
                if (reservations.First().Instances.First().PublicIpAddress != null) ip = reservations.First().Instances.First().PublicIpAddress;
                if (reservations.First().Instances.First().State.Name == InstanceStateName.Running) state = InstanceStateName.Running;
            }
            var instance = new SnapperInstance(instanceInfo.Key, ip);
            instance.sshManager = new SSHManager(instanceInfo.Value, ip, password);
            
            switch (instanceInfo.Value)
            {
                case SnapperRoleType.Controller:
                    snapper.controller = instance;
                    break;
                case SnapperRoleType.GlobalSilo:
                    snapper.globalSilo = instance;
                    break;
                case SnapperRoleType.RegionalSilo:
                    snapper.regionalSilos.Add(instance);
                    break;
                case SnapperRoleType.LocalSilo:
                    snapper.localSilos.Add(instance);
                    break;
                case SnapperRoleType.Worker:
                    snapper.workers.Add(instance);
                    break;
                case SnapperRoleType.LocalReplicaSilo:
                    snapper.localReplicaSilos.Add(instance);
                    break;
                case SnapperRoleType.ReplicaWorker:
                    snapper.replicaWorkers.Add(instance);
                    break;
            }
        }
        Console.WriteLine($"All instances are started");
        return snapper;
    }

    public async Task StopAllInstances()
    {
        await client.StopInstancesAsync(new StopInstancesRequest { InstanceIds = instanceIDs });

        // wait until all instances are in the state "stopped"
        foreach (var id in instanceIDs)
        {
            InstanceStateName state = InstanceStateName.Pending;
            while (state != InstanceStateName.Stopped)
            {
                state = (await client.DescribeInstancesAsync(new DescribeInstancesRequest { InstanceIds = new List<string> { id } })).Reservations.First().Instances.First().State.Name;
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        }
    }

    public async Task SpawnNew(int maxNumRegion, int maxNumLocalSilo, int maxNumLocalReplicaSilo)
    {
        Debug.Assert(maxNumRegion == 1);

        // create a new file to store the instance info
        if (File.Exists(Constants.localInstanceInfo)) File.Delete(Constants.localInstanceInfo);
        var file = new StreamWriter(Constants.localInstanceInfo, true);

        // search for the template ID (the template has .NET + OpenSSH Server installed and enabled)
        var templates = (await client.DescribeLaunchTemplatesAsync(new DescribeLaunchTemplatesRequest { LaunchTemplateNames = new List<string> { "snapper-template" } })).LaunchTemplates;
        Debug.Assert(templates.Count == 1);
        var templateID = templates.First().LaunchTemplateId;
        Console.WriteLine($"templateID = {templateID}, version = {templates.First().DefaultVersionNumber}");

        // spawn instance for controller
        {
            Console.WriteLine($"EC2Manager: Launch instance for {SnapperRoleType.Controller}...");
            var controllerType = GetInstanceType(2);
            var id = (await LaunchInstances(templateID, controllerType, 1)).First();
            await file.WriteLineAsync($"{SnapperRoleType.Controller} {id}");
        }

        // spawn instance for global silo
        {
            Console.WriteLine($"EC2Manager: Launch instance for {SnapperRoleType.GlobalSilo}...");
            var globalSiloType = GetInstanceType(maxNumRegion * maxNumLocalSilo);
            var id = (await LaunchInstances(templateID, globalSiloType, 1)).First();
            await file.WriteLineAsync($"{SnapperRoleType.GlobalSilo} {id}");
        }

        // spawn instance for regional silo
        {
            Console.WriteLine($"EC2Manager: Launch instance for {SnapperRoleType.RegionalSilo}...");
            var globalSiloType = GetInstanceType(maxNumLocalSilo);
            var id = (await LaunchInstances(templateID, globalSiloType, maxNumRegion)).First();
            await file.WriteLineAsync($"{SnapperRoleType.RegionalSilo} {id}");
        }

        // spawn instances for local silos and workers
        var num = maxNumRegion * maxNumLocalSilo * 2;
        var ids = await LaunchInstances(templateID, InstanceType.C5nXlarge, num);
        for (var i = 0; i < ids.Count; i++)
        {
            SnapperRoleType type;
            if (i < maxNumRegion * maxNumLocalSilo)
            {
                Console.WriteLine($"EC2Manager: Launch instance {i} for {SnapperRoleType.LocalSilo}...");
                type = SnapperRoleType.LocalSilo;
            }
            else
            {
                Console.WriteLine($"EC2Manager: Launch instance {i} for {SnapperRoleType.Worker}...");
                type = SnapperRoleType.Worker;
            } 
            await file.WriteLineAsync($"{type} {ids[i]}");
        }

        // spawn instances for local replica silos and replica workers
        num = maxNumRegion * maxNumLocalReplicaSilo * 2;
        if (num != 0)
        {
            ids = await LaunchInstances(templateID, InstanceType.C5nXlarge, maxNumRegion * maxNumLocalReplicaSilo * 2);
            for (var i = 0; i < ids.Count; i++)
            {
                SnapperRoleType type;
                if (i < maxNumRegion * maxNumLocalReplicaSilo)
                {
                    Console.WriteLine($"EC2Manager: Launch instance {i} for {SnapperRoleType.LocalReplicaSilo}...");
                    type = SnapperRoleType.LocalReplicaSilo;
                }
                else
                {
                    Console.WriteLine($"EC2Manager: Launch instance {i} for {SnapperRoleType.ReplicaWorker}...");
                    type = SnapperRoleType.ReplicaWorker;
                }
                await file.WriteLineAsync($"{type} {ids[i]}");
            }
        }
        
        file.Close();
    }

    public async Task<string> GetSecurityGroupID()
    {
        var groups = (await client.DescribeSecurityGroupsAsync(new DescribeSecurityGroupsRequest { GroupNames = new List<string> { "snapper" } })).SecurityGroups;
        Debug.Assert(groups.Count == 1);
        Debug.Assert(groups.First().GroupName == "snapper");
        return groups.First().GroupId;
    }

    async Task<List<string>> LaunchInstances(string templateID, InstanceType type, int numInstances)
    {
        var req = new RunInstancesRequest
        {
            LaunchTemplate = new LaunchTemplateSpecification { LaunchTemplateId = templateID },
            InstanceType = type,
            MinCount = numInstances,
            MaxCount = numInstances
        };
        
        // launch the instance
        var instances = (await client.RunInstancesAsync(req)).Reservation.Instances;
        var instanceIDs = new List<string>();
        foreach (var instance in instances) instanceIDs.Add(instance.InstanceId);
        return instanceIDs;
    }

    InstanceType GetInstanceType(int cpu)
    {
        if (cpu <= 4) return InstanceType.C5nXlarge;              // 4 cpu
        if (cpu > 4 && cpu <= 8) return InstanceType.C5n2xlarge;  // 8 cpu
        if (cpu > 8 && cpu <= 16) return InstanceType.C5n4xlarge; // 16 cpu
        throw new Exception($"Exception: Unsupported cpu = {cpu}");
    }
}
using Experiment.Accelerator;
using System.Diagnostics;
using Utilities;

// STEP 1: manually launch an instance
//         (1) use image Windows Server 2022 Base
//         (2) create and use the key-pair "snapper-ami"
//         (3) create and use the security group "snapper"
//             - inbound: (TCP 22) allow ssh connection from my local pc
//             - inbound: (TCP 3389) allow RDP connection from my local pc
//             - inbound: (TCP 6379) allow connection to redis cluster
//             - inbound: (TCP 5554 - 5558) allow connection between instances
//             - inbound: (TCP 11111 - 11131) allow traffic between silo ports
//             - inbound: (TCP 30000 - 30020) allow traffic between silo gateway ports
//             - outbound: all allowed
//         (4) create and use the (cluster) placement group "snapper"
//             - cluster placement
//         (5) 40GB gp2 storage
// STEP 2: install .NET SDK on the instance, install OpenSSH Server and enable it on the instance
// STEP 3: create an image "snapper-ami" of this instance
// STEP 4: create a launch template
//         (1) it uses the "snapper-ami" as image
//         (2) it uses the key-pair "snapper-ami"
//         (3) it uses "snapper" security group
//         (4) it uses "snapper" placement group
// STEP 5: in the code, all instances are launched by using the created template
//         (1) need to specify the instance type (large or xlarge) in the code

var maxNumRegion = 1;
var maxNumLocalSilo = 16;
var maxNumLocalReplicaSilo = 0;
var reInvokeAllResources = false;

// ========================================================================================================
// read credential info from file and create a EC2 client
string accessKey, secretKey, globalRegion, password;
var regionList = new List<string>();    // do not include the global region
using (var file = new StreamReader(Constants.localCredentialFile))
{
    accessKey = file.ReadLine();
    secretKey = file.ReadLine();
    globalRegion = file.ReadLine();
    Debug.Assert(!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey) && !string.IsNullOrEmpty(globalRegion));

    for (var i = 0; i < maxNumRegion; i++)
    {
        var region = file.ReadLine();
        Debug.Assert(!string.IsNullOrEmpty(region));
        regionList.Add(region);
    }
    
    password = file.ReadLine();
    Debug.Assert(!string.IsNullOrEmpty(password));
}

var ec2Manager = new EC2Manager(accessKey, secretKey);
var redisManager = new RedisManager(accessKey, secretKey);
var dynamoManager = new DynamoManager(accessKey, secretKey);

// ========================================================================================================
if (reInvokeAllResources)
{
    await ec2Manager.CleanUp();

    await ec2Manager.SpawnNew(maxNumRegion, maxNumLocalSilo, maxNumLocalReplicaSilo);
    var securityGroupID = await ec2Manager.GetSecurityGroupID();
    await redisManager.SpawnNew(securityGroupID);

    Console.WriteLine("All instances are re-invoked, wait for 5m so the instances are ready");
    Thread.Sleep(TimeSpan.FromMinutes(5));
}

await dynamoManager.CleanUp();
await redisManager.CleanUp();

// ========================================================================================================
var snapper = await ec2Manager.StartAllInstances(password, maxNumRegion, maxNumLocalSilo, maxNumLocalReplicaSilo);
Debug.Assert(snapper.localSilos.Count == maxNumRegion * maxNumLocalSilo);
Debug.Assert(snapper.workers.Count == maxNumRegion * maxNumLocalSilo);
Debug.Assert(snapper.localReplicaSilos.Count == maxNumRegion * maxNumLocalReplicaSilo);
Debug.Assert(snapper.replicaWorkers.Count == maxNumRegion * maxNumLocalReplicaSilo);

// ========================================================================================================
// clean the local files
//var file1 = File.Create(Constants.localWorkDir + @"data\result.txt");
//file1.Close();
var file2 = File.Create(Constants.localWorkDir + @"data\tp-GrainMigration-100%PACT.txt");
file2.Close();
var file3 = File.Create(Constants.localWorkDir + @"data\tp-GrainMigration-50%PACT.txt");
file3.Close();
var file4 = File.Create(Constants.localWorkDir + @"data\tp-GrainMigration-0%PACT.txt");
file4.Close();
if (Directory.Exists(Constants.localWorkDir + @"data\log")) Directory.Delete(Constants.localWorkDir + @"data\log", true);

// ========================================================================================================
var kafka = "kafka";
var controller_public_ip = snapper.controller.publicIP;
var experiments = BasicConfigurationParser.GetBasicConfigurationFromXMLFile();
Console.WriteLine($"controller public IP = {controller_public_ip}");
Console.WriteLine($"memory Db connection string = {redisManager.connectionString}");

var barriers = new CountdownEvent[experiments.Count];
for (int i = 0; i < barriers.Length; i++) barriers[i] = new CountdownEvent(snapper.GetTotalNumServices());

Console.WriteLine($"Start thread {SnapperRoleType.Controller}");
snapper.controller.sshManager.SetBarriers(barriers);
var controllerThread = new Thread(snapper.controller.sshManager.ThreadWork);
controllerThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, 0), experiments));

Console.WriteLine($"Start thread {SnapperRoleType.GlobalSilo}");
snapper.globalSilo.sshManager.SetBarriers(barriers);
var globalSiloThread = new Thread(snapper.globalSilo.sshManager.ThreadWork);
globalSiloThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, 0), experiments));

var index = 0;
var regionalSiloThreads = new List<Thread>();
foreach (var regionalSilo in snapper.regionalSilos)
{
    Console.WriteLine($"Start thread {SnapperRoleType.RegionalSilo}-{index}");
    regionalSilo.sshManager.SetBarriers(barriers);
    var regionalSiloThread = new Thread(regionalSilo.sshManager.ThreadWork);
    regionalSiloThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, index), experiments));
    regionalSiloThreads.Add(regionalSiloThread);
    index++;
}

index = 0;
var localSiloThreads = new List<Thread>();
foreach (var localSilo in snapper.localSilos)
{
    Console.WriteLine($"Start thread {SnapperRoleType.LocalSilo}-{index}");
    localSilo.sshManager.SetBarriers(barriers);
    var localSiloThread = new Thread(localSilo.sshManager.ThreadWork);
    localSiloThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, index), experiments));
    localSiloThreads.Add(localSiloThread);
    index++;
}

index = 0;
var workerThreads = new List<Thread>();
foreach (var worker in snapper.workers)
{
    Console.WriteLine($"Start thread {SnapperRoleType.Worker}-{index}");
    worker.sshManager.SetBarriers(barriers);
    var workerThread = new Thread(worker.sshManager.ThreadWork);
    workerThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, index), experiments));
    workerThreads.Add(workerThread);
    index++;
}

index = 0;
var localReplicaSiloThreads = new List<Thread>();
foreach (var localReplicaSilo in snapper.localReplicaSilos)
{
    Console.WriteLine($"Start thread {SnapperRoleType.LocalReplicaSilo}-{index}");
    localReplicaSilo.sshManager.SetBarriers(barriers);
    var localReplicaSiloThread = new Thread(localReplicaSilo.sshManager.ThreadWork);
    localReplicaSiloThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, index), experiments));
    localReplicaSiloThreads.Add(localReplicaSiloThread);
    index++;
}

index = 0;
var replicaWorkerThreads = new List<Thread>();
foreach (var replicaWorker in snapper.replicaWorkers)
{
    Console.WriteLine($"Start thread {SnapperRoleType.ReplicaWorker}-{index}");
    replicaWorker.sshManager.SetBarriers(barriers);
    var replicaWorkerThread = new Thread(replicaWorker.sshManager.ThreadWork);
    replicaWorkerThread.Start((new InputParameters(redisManager.connectionString, kafka, controller_public_ip, index), experiments));
    replicaWorkerThreads.Add(replicaWorkerThread);
    index++;
}

controllerThread.Join();
globalSiloThread.Join();
foreach (var regionalSiloThread in regionalSiloThreads) regionalSiloThread.Join();
foreach (var localSiloThread in localSiloThreads) localSiloThread.Join();
foreach (var workerThread in workerThreads) workerThread.Join();
foreach (var localReplicaSiloThread in localReplicaSiloThreads) localReplicaSiloThread.Join();
foreach (var replicaWorkerThread in replicaWorkerThreads) replicaWorkerThread.Join();
Console.WriteLine($"All threads are done");

// ========================================================================================================
// need to wait until the experiments are done
//await ec2Manager.StopAllInstances();
//Console.WriteLine("all insatnces are stopped");
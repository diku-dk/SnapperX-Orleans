using Renci.SshNet;
using System.Diagnostics;
using Utilities;

namespace Experiment.Accelerator;

internal class SSHManager
{
    readonly string processName;
    readonly SnapperRoleType type;
    readonly SshClient sshClient;
    readonly ScpClient scpClient;
    CountdownEvent[] barriers;

    public SSHManager(SnapperRoleType type, string host, string password)
    {
        this.type = type;
        switch (type)
        {
            case SnapperRoleType.Controller:
                processName = "Experiment.Controller";
                break;
            case SnapperRoleType.GlobalSilo:
            case SnapperRoleType.RegionalSilo:
            case SnapperRoleType.LocalSilo:
            case SnapperRoleType.LocalReplicaSilo:
                processName = "SnapperSiloHost";
                break;
            case SnapperRoleType.Worker:
            case SnapperRoleType.ReplicaWorker:
                processName = "Experiment.Worker";
                break;
        }
        sshClient = new SshClient(host, "Administrator", password);
        scpClient = new ScpClient(host, "Administrator", password);
    }

    public void SetBarriers(CountdownEvent[] barriers) => this.barriers = barriers;
    
    public void ThreadWork(object obj)
    {
        Connect();
        Console.WriteLine($"{type}: connected to instance, now transfer codes...");
        TerminateProcess();
        DeployConfigureFile();
        //DeployCode();

        (var input, var experiments ) = ((InputParameters, List<BasicConfiguration>))obj;
        Debug.Assert(input != null);

        for (int i = 0; i < experiments.Count; i++)
        {
            var exp = experiments[i];

            var numNeededInstance = 1;
            switch (type)
            {
                case SnapperRoleType.RegionalSilo: 
                    numNeededInstance = exp.numRegion; break;
                case SnapperRoleType.LocalSilo:
                case SnapperRoleType.Worker:
                    numNeededInstance = exp.numRegion * exp.numSiloPerRegion; break;
                case SnapperRoleType.LocalReplicaSilo:
                case SnapperRoleType.ReplicaWorker:
                    numNeededInstance = exp.numRegion * exp.numReplicaSiloPerRegion; break;
            }

            Connect();
            barriers[i].Signal();
            barriers[i].Wait();           // all threads start at the same time

            if (input.instanceIndex < numNeededInstance)
            {
                if (type == SnapperRoleType.Controller)
                {
                    Console.WriteLine($"{type}: wait for 5s...");
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
                else
                {
                    Console.WriteLine($"{type}: wait for 10s...");
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                }

                Console.WriteLine($"{type}: run experiment: experimentID = {exp.experimentID}, index1 = {exp.experimentIndex1}, index2 = {exp.experimentIndex2}");
                RunCode(input, exp);
            }

            if (type == SnapperRoleType.Controller)
            {
                Console.WriteLine($"{type}: downloading experiment result");
                Connect();
                GetResult();
            }

            if (input.instanceIndex < numNeededInstance) TerminateProcess();
        }

        Disconnect();
        Console.WriteLine($"{type}: disconnect the instance");
    }

    void Connect()
    {
        var succeed = false;
        while (succeed == false)
        {
            try
            {
                if (sshClient.IsConnected == false) sshClient.Connect();
                if (scpClient.IsConnected == false) scpClient.Connect();
                succeed = true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.Message} {e.StackTrace}");
                Console.WriteLine("Fail to connect, wait for 5s, and try again...");
                Thread.Sleep(TimeSpan.FromSeconds(5));   // try it again after 5s
            }
        }
    }

    void TerminateProcess()
    {
        // force to kill the running process which may be using the source code
        var res = sshClient.RunCommand($"taskkill.exe /F /IM {processName}.exe");
        //if (res.Error.Length != 0) Console.WriteLine($"Kill the process for {type}, {res.Error}");
    }

    void DeployCode()
    {
        // remove the exisiting Snapper directory
        var res = sshClient.RunCommand($"rmdir /Q /S {Constants.workDir}");
        //if (res.Error.Length != 0) Console.WriteLine($"Remove the directory for {type}, {res.Error}");

        // create a new empty directory
        res = sshClient.RunCommand($"mkdir {Constants.workDir}");
        //if (res.Error.Length != 0) Console.WriteLine($"Create the directory for {type}, {res.Error}");

        // transfer all files from local to remote
        var directoryInfo = new DirectoryInfo(Constants.localWorkDir);
        scpClient.Upload(directoryInfo, Constants.workDir);
    }

    void DeployConfigureFile()
    {
        // remove the existing data directory
        var res = sshClient.RunCommand($"rmdir /Q /S {Constants.dataDir}");

        // create a new empty directory
        res = sshClient.RunCommand($"mkdir {Constants.dataDir}");

        // transfer all files from local to remote
        var directoryInfo = new DirectoryInfo(Constants.localDataPath);
        scpClient.Upload(directoryInfo, Constants.dataDir);
    }

    void RunCode(InputParameters input, BasicConfiguration exp)
    {
        string cmd;
        switch (type)
        {
            case SnapperRoleType.Controller:
                cmd = @$"dotnet run --project {Constants.workDir}\Experiment.Controller {exp.experimentID} {exp.experimentIndex1} {exp.experimentIndex2} {input.redis_ConnectionString} {input.kafka_ConnectionString}";
                break;
            case SnapperRoleType.GlobalSilo:
            case SnapperRoleType.RegionalSilo:
            case SnapperRoleType.LocalSilo:
            case SnapperRoleType.LocalReplicaSilo:
                cmd = @$"dotnet run --project {Constants.workDir}\SnapperSiloHost false {type} eu-north-1 {input.controller_PublicIP}";
                break;
            case SnapperRoleType.Worker:
            case SnapperRoleType.ReplicaWorker:
                cmd = @$"dotnet run --project {Constants.workDir}\Experiment.Worker false {type} eu-north-1 {input.controller_PublicIP}";
                break;
            default:
                throw new Exception($"Unsupported instance type {type}");
        }

        var command = sshClient.CreateCommand(cmd);
        var asyncExe = command.BeginExecute();

        switch (type)
        {
            case SnapperRoleType.Controller: PrintConsole(command, asyncExe); break;
            case SnapperRoleType.GlobalSilo: PrintConsole(command, asyncExe); break;
            case SnapperRoleType.RegionalSilo: PrintConsole(command, asyncExe); break;
            case SnapperRoleType.LocalSilo: PrintConsole(command, asyncExe); break;
            case SnapperRoleType.LocalReplicaSilo: PrintConsole(command, asyncExe); break;
            case SnapperRoleType.Worker: PrintConsole(command, asyncExe); break;
            case SnapperRoleType.ReplicaWorker: PrintConsole(command, asyncExe); break;
        }

        command.EndExecute(asyncExe);     // wait until the command is completed
        Console.WriteLine($"Finish experiment on {type}");
    }

    void PrintConsole(SshCommand command, IAsyncResult asyncExe)
    {
        // read console output data
        string line;
        var flag = false;
        var reader = new StreamReader(command.OutputStream);
        while (asyncExe.IsCompleted == false)
        {
            line = reader.ReadToEnd();
            if (flag == false && line.Contains(GetString(type))) flag = true;
            if (flag && line.Length != 0) Console.Write(line);
        }
        line = reader.ReadToEnd();
        if (flag && line.Length != 0) Console.Write(line);
    }

    /// <returns> the first console print by different roles </returns>
    string GetString(SnapperRoleType type)
    {
        switch (type)
        {
            case SnapperRoleType.Controller: 
                return $"{type}: experimentID = ";
            case SnapperRoleType.GlobalSilo:
            case SnapperRoleType.RegionalSilo:
            case SnapperRoleType.LocalSilo:
            case SnapperRoleType.LocalReplicaSilo:
            case SnapperRoleType.Worker:
            case SnapperRoleType.ReplicaWorker:
                return $"{type} ==> {SnapperRoleType.Controller}";
            default:
                throw new Exception($"Unsupported instance type {type}");
        }
    }

    void GetResult()
    {
        var outputFile1 = new FileInfo($@"{Constants.localResultPath}");
        scpClient.Download($@"{Constants.dataPath}result.txt", outputFile1);

        var fileName = "tp-GrainMigration-100%PACT.txt";
        var outputFile2 = new FileInfo(Constants.localDataPath + fileName);
        scpClient.Download(Constants.dataPath + fileName, outputFile2);

        fileName = "tp-GrainMigration-50%PACT.txt";
        var outputFile3 = new FileInfo(Constants.localDataPath + fileName);
        scpClient.Download(Constants.dataPath + fileName, outputFile3);

        fileName = "tp-GrainMigration-0%PACT.txt";
        var outputFile4 = new FileInfo(Constants.localDataPath + fileName);
        scpClient.Download(Constants.dataPath + fileName, outputFile4);
    }

    void Disconnect()
    {
        sshClient.Disconnect();
        scpClient.Disconnect();
    }
}
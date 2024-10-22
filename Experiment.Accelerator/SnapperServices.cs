namespace Experiment.Accelerator;

internal class SnapperServices
{
    public SnapperInstance controller;
    public SnapperInstance globalSilo;
    public List<SnapperInstance> regionalSilos = new List<SnapperInstance>();
    public List<SnapperInstance> localSilos = new List<SnapperInstance>();
    public List<SnapperInstance> workers = new List<SnapperInstance>();
    public List<SnapperInstance> localReplicaSilos = new List<SnapperInstance>();
    public List<SnapperInstance> replicaWorkers = new List<SnapperInstance>();

    public int GetTotalNumServices() => 1 + 1 + regionalSilos.Count + localSilos.Count + workers.Count + localReplicaSilos.Count + replicaWorkers.Count;
}

internal class SnapperInstance
{
    public readonly string instanceID;
    public readonly string publicIP;
    public SSHManager sshManager { get; set; }

    public SnapperInstance(string instanceID, string publicIP)
    {
        this.instanceID = instanceID;
        this.publicIP = publicIP;
    }
}
using System.Collections.Concurrent;
using System.Diagnostics;
using Utilities;
using Concurrency.Common.Cache;
using Experiment.Common;

namespace Experiment.Worker;

internal class SingleExperimentManager
{
    readonly bool isReplica;
    readonly string myRegionID;
    readonly string mySiloID;
    readonly string myReplicaSiloID;
    readonly BenchmarkType benchmark;
    readonly ImplementationType implementationType;
    readonly IWorkloadConfigure workload;
    readonly IEnvConfigure envConfigureHelper;
    readonly StaticClusterInfo staticClusterInfo;
    readonly StaticReplicaInfo? staticReplicaInfo;
    readonly List<IClusterClient> clients;

    int actPipeSize;
    int pactPipeSize;
    int numActConsumer;
    int numPactConsumer;

    int numEpoch;
    int migrationPipeSize;
    int numMigrationConsumer;

    Dictionary<int, Queue<(bool, RequestData)>> shared_requests;                      // epoch ID, (isDet, txn)
    Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>> thread_requests;   // epoch ID, <consumer thread ID, queue of txn>>

    Barrier[] barriers;
    CountdownEvent[] threadAcks;
    Thread producerThread;
    Thread[] consumerThreads;
    Thread[] grainMigrationThreads;
    bool[] isEpochFinish;
    bool[] isProducerFinish;
    WorkloadResult[] results;

    public SingleExperimentManager(
        bool isReplica,
        string myRegionID,
        string mySiloID,
        string myReplicaSiloID,
        int regionIndex,
        int siloIndex,
        IEnvSetting envSetting,
        IEnvConfigure envConfigureHelper,
        IWorkloadConfigure workload,
        List<IClusterClient> clients,
        StaticClusterInfo staticClusterInfo,
        StaticReplicaInfo? staticReplicaInfo)
    {
        this.isReplica = isReplica;
        this.myRegionID = myRegionID;
        this.mySiloID = mySiloID;
        this.myReplicaSiloID = myReplicaSiloID;
        this.workload = workload;
        this.clients = clients;
        var basicEnvSetting = envSetting.GetBasic();
        benchmark = basicEnvSetting.benchmark;
        implementationType = basicEnvSetting.implementationType;
        this.envConfigureHelper = envConfigureHelper;
        this.staticClusterInfo = staticClusterInfo;
        this.staticReplicaInfo = staticReplicaInfo;

        // =========================================================================================================================
        actPipeSize = workload.GetActPipeSize();
        pactPipeSize = workload.GetPactPipeSize();
        numActConsumer = workload.GetNumActConsumer();
        numPactConsumer = workload.GetNumPactConsumer();

        (numEpoch, _) = basicEnvSetting.GetNumEpochAndNumReRun();
        migrationPipeSize = workload.GetMigrationPipeSize();
        numMigrationConsumer = workload.GetNumMigrationConsumer();

        if (pactPipeSize == 100) numActConsumer = 0;
        else if (pactPipeSize == 0) numPactConsumer = 0;

        if (migrationPipeSize == 0) numMigrationConsumer = 0;

        Console.WriteLine($"consumer: pact = {numPactConsumer}, act = {numActConsumer}, migration = {numMigrationConsumer}");

        barriers = new Barrier[numEpoch];
        threadAcks = new CountdownEvent[numEpoch];
        for (int i = 0; i < numEpoch; i++)
        {
            barriers[i] = new Barrier(numPactConsumer + numActConsumer + numMigrationConsumer + 1);
            threadAcks[i] = new CountdownEvent(numPactConsumer + numActConsumer + numMigrationConsumer);
        } 
        
        thread_requests = new Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>>();
        for (int epoch = 0; epoch < numEpoch; epoch++)
        {
            thread_requests.Add(epoch, new Dictionary<int, ConcurrentQueue<RequestData>>());
            for (int t = 0; t < numPactConsumer + numActConsumer; t++) thread_requests[epoch].Add(t, new ConcurrentQueue<RequestData>());
        }

        isEpochFinish = new bool[numEpoch];
        isProducerFinish = new bool[numEpoch];
        for (int e = 0; e < numEpoch; e++)
        {
            isEpochFinish[e] = false;     // when worker thread finishes an epoch, set true
            isProducerFinish[e] = false;
        }

        results = new WorkloadResult[numPactConsumer + numActConsumer];

        // =========================================================================================================================
        // generate enough number of transactions for every epoch
        var localReplicaSiloList = new List<string>();
        if (basicEnvSetting.inRegionReplication || basicEnvSetting.crossRegionReplication) localReplicaSiloList = staticReplicaInfo.localReplicaSiloListPerRegion[myRegionID];

        IWorkloadGenerator workloadGenerator;
        switch (benchmark)
        {
            case BenchmarkType.SMALLBANK:
                workloadGenerator = new SmallBank.Workload.WorkloadGenerator(isReplica, myRegionID, regionIndex, siloIndex, envSetting, envConfigureHelper, (SmallBank.Workload.WorkloadConfigure)workload, localReplicaSiloList);
                break;
            case BenchmarkType.MARKETPLACE:
                workloadGenerator = new MarketPlace.Workload.WorkloadGenerator(isReplica, myRegionID, mySiloID, regionIndex, siloIndex, envSetting, envConfigureHelper, (MarketPlace.Workload.WorkloadConfigure)workload, localReplicaSiloList);
                break;
            default:
                throw new Exception($"SingleExperimentManager: the {benchmark} benchmark is not supported yet. ");
        }

        shared_requests = workloadGenerator.GenerateSimpleWorkload();

        // =========================================================================================================================
        // init producer thread
        producerThread = new Thread(ProducerThreadWork);
        producerThread.Start();

        // =========================================================================================================================
        // init a group of orleans clients
        var taskIsDone = new CountdownEvent(1);
        InitClients(taskIsDone, basicEnvSetting.accessKey, basicEnvSetting.secretKey);
        taskIsDone.Wait();

        // =========================================================================================================================
        // init grain migration threads
        grainMigrationThreads = new Thread[numMigrationConsumer];
        for (int i = 0; i < numMigrationConsumer; i++)
        {
            var thread = new Thread(GrainMigrationThreadWorkAsync);
            grainMigrationThreads[i] = thread;
            thread.Start(i);
        }

        // =========================================================================================================================
        // init consumer threads
        consumerThreads = new Thread[numPactConsumer + numActConsumer];
        for (int i = 0; i < numPactConsumer + numActConsumer; i++)
        {
            var thread = new Thread(ConsumerThreadWorkAsync);
            consumerThreads[i] = thread;
            if (i < numPactConsumer) thread.Start((i, true));
            else thread.Start((i, false));
        }
    }

    async void InitClients(CountdownEvent taskIsDone, string accessKey, string secretKey)
    {
        var numClient = clients.Count();

        for (int i = 0; i < numPactConsumer + numActConsumer + numMigrationConsumer - numClient; i++)
            clients.Add(await OrleansClientManager.GetOrleansClient(myRegionID, accessKey, secretKey, implementationType));
        taskIsDone.Signal();
    }

    void ProducerThreadWork(object _)
    {
        ProducerThread.Run(numEpoch, numPactConsumer, numActConsumer, pactPipeSize * 10, actPipeSize * 10,
            isEpochFinish, isProducerFinish, shared_requests, thread_requests);
    }

    async void ConsumerThreadWorkAsync(object obj)
    {
        var input = ((int, bool))obj;
        var threadIndex = input.Item1;
        var isDet = input.Item2;

        var client = clients[threadIndex];
        var pipeSize = isDet ? pactPipeSize : actPipeSize;
        
        Console.WriteLine($"Consumer thread = {threadIndex}, isDet = {isDet}, pipe = {pipeSize}");
        for (int eIndex = 0; eIndex < numEpoch; eIndex++)
        {
            var queue = thread_requests[eIndex][threadIndex];
            var isMigrationExp = numEpoch == 1;
            var consumer = new ConsumerThread(isReplica, myRegionID, mySiloID, myReplicaSiloID, isMigrationExp, envConfigureHelper, staticClusterInfo, staticReplicaInfo, isDet, benchmark, implementationType, workload, client, queue, barriers, isEpochFinish, isProducerFinish);
            results[threadIndex] = await consumer.RunEpoch(eIndex);
            threadAcks[eIndex].Signal();  // Signal the completion of epoch
        }
    }

    async void GrainMigrationThreadWorkAsync(object obj)
    {
        var threadIndex = (int)obj;
        var index = numPactConsumer + numActConsumer + threadIndex;
        var client = clients[index];

        Debug.Assert(numEpoch == 1);
        var consumer = new GrainMigrationThread(isReplica, migrationPipeSize, client, envConfigureHelper, barriers);

        try
        {
            var eIndex = 0;
            await consumer.RunEpoch(eIndex, threadIndex);
            threadAcks[0].Signal();        // Signal the completion of epoch
        }
        catch (Exception e)
        {
            Console.WriteLine($"Exception: {e.Message} {e.StackTrace}");
            throw;
        }
        Console.WriteLine($"Migration thread signal");
        
    }

    public WorkloadResult[] RunEpoch(int epoch)
    {
        barriers[epoch].SignalAndWait();
        threadAcks[epoch].Wait();
        return results;
    }
}
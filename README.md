=================================================

- Paper title: Rethinking State Management in Actor Systems for Cloud-Native Applications
- DOI: https://doi.org/10.1145/3698038.3698540

# SnapperX-Orleans (SoCC 2024)

SnapperX is a data management layer for actor systems, allowing developers to declare data dependencies that cut across actors, including foreign keys, data replication, and other dependencies. SnapperX can transparently enforce the declared dependencies, reducing the burden on developers. Furthermore, SnapperX employs novel logging and concurrency control algorithms to support transactional maintenance of data dependencies.

## Environment Deployment
- install dotnet sdk 8.0.300
- install Docker Desktop
- Reset the global and local region in file /data/AWS_credential.txt (It's the place to store Orleans membership table)

## Configure Workload
- Configuration file: /data/XML/Exp1-SNAPPER.xml
- Configurable variables: 
  multiSiloPercent = [0, 100], the percentage of transactions the span multiple silos
  pactPipeSize >= 1, the max number of concurrent transactions submitted to the system 

## Run experiment (test mode)
0. start the Redis container
- `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`
- `.\RunRedis.ps1`

1. run controller
- `dotnet run --project .\Experiment.Controller`

2. run global silo
- `dotnet run --project .\SnapperSiloHost true GlobalSilo eu-north-1 localhost 0`

3. run regional silo
- `dotnet run --project .\SnapperSiloHost true RegionalSilo eu-north-1 localhost 1`

4. run 2 local silo
- `dotnet run --project .\SnapperSiloHost true LocalSilo eu-north-1 localhost 2`
- `dotnet run --project .\SnapperSiloHost true LocalSilo eu-north-1 localhost 3`

5. run 2 local replica silo
- `dotnet run --project .\SnapperSiloHost true LocalReplicaSilo eu-north-1 localhost 4`
- `dotnet run --project .\SnapperSiloHost true LocalReplicaSilo eu-north-1 localhost 5`

5. run 2 workers
- `dotnet run --project .\Experiment.Worker true Worker eu-north-1 localhost 6`
- `dotnet run --project .\Experiment.Worker true Worker eu-north-1 localhost 7`

6. run 2 replica workers (if needed)
- `dotnet run --project .\Experiment.Worker true ReplicaWorker eu-north-1 localhost 8`
- `dotnet run --project .\Experiment.Worker true ReplicaWorker eu-north-1 localhost 9`

kill a process: `taskkill /F /PID 1234`

## Check Experiment Result
- The result is output in file /data/result.txt
- How to read the result: check code of /Experiment.Worker/ExperimentResultAggregator.cs (line 59 - 149)
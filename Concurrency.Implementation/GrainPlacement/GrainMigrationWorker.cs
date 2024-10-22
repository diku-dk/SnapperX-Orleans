using Concurrency.Common;
using Concurrency.Common.ICache;
using Concurrency.Interface;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Microsoft.CodeAnalysis;
using Orleans.Concurrency;
using StackExchange.Redis;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.GrainPlacement;

[Reentrant]
[SnapperGrainPlacementStrategy]
public class GrainMigrationWorker : Grain, IGrainMigrationWorker
{
    SnapperGrainID myID;
    readonly bool speculativeACT;
    readonly bool speculativeBatch;
    readonly ISnapperClusterCache snapperClusterCache;
    readonly IScheduleManager scheduleManager;
    readonly IDatabase grainPlacementChange_db;

    HashSet<GrainID> grainToMigrate;
    List<IGrainPlacementManager> allPMs;

    public GrainMigrationWorker(ISnapperClusterCache snapperClusterCache, IScheduleManager scheduleManager, IConnectionMultiplexer redis)
    {
        this.snapperClusterCache = snapperClusterCache;
        this.scheduleManager = scheduleManager;
        (speculativeACT, speculativeBatch) = snapperClusterCache.IsSpeculative();
        grainPlacementChange_db = redis.GetDatabase(Constants.Redis_GrainPlacementChange);
    }

    public Task<AggregatedExperimentData> CheckGC()
    {
        if (grainToMigrate.Count != 0) Console.WriteLine($"GrainMigrationWorker: grainToMigrate.Count = {grainToMigrate.Count}");
        return Task.FromResult(new AggregatedExperimentData());
    }

    public override Task OnActivateAsync(CancellationToken _)
    {
        var strs = this.GetPrimaryKeyString().Split('+');

        var guid = Guid.Parse(strs[0]);
        var myRegionID = strs[1];
        Debug.Assert(strs[2].Equals(RuntimeIdentity));
        var mySiloID = strs[2];

        myID = new SnapperGrainID(guid, myRegionID + "+" + mySiloID, typeof(GrainMigrationWorker).FullName);
        grainToMigrate = new HashSet<GrainID>();
        allPMs = new List<IGrainPlacementManager>();
        var allPMIDs = snapperClusterCache.GetAllGrainPlacementManagers().Select(tuple => new SnapperGrainID(tuple.Item1, tuple.Item2, typeof(GrainPlacementManager).FullName)).ToList();
        foreach (var id in allPMIDs) allPMs.Add(GrainFactory.GetGrain<IGrainPlacementManager>(id.grainID.id, id.location, id.grainID.className));

        return Task.CompletedTask;
    }

    public async Task<GrainMigrationResult> MigrateGrain(GrainID grainID, int targetPartitionID)
    {
        try
        {
            Debug.Assert(speculativeBatch);    // grain migration cannot work if speculative batch execution is not enabled

            // check if this MigrationWorker is the correct one to handle this request
            (var mwGuid, var mwLocation) = snapperClusterCache.GetGrainMigrationWorker(grainID);
            if (mwGuid == myID.grainID.id && mwLocation == myID.location) return await DoMigration(grainID, targetPartitionID);
            else
            {
                var worker = GrainFactory.GetGrain<IGrainMigrationWorker>(mwGuid, mwLocation, typeof(GrainMigrationWorker).FullName);
                return await worker.DoMigration(grainID, targetPartitionID);
            }
        }
        catch (SnapperException e)
        {
            Debug.Assert(e.exceptionType == ExceptionType.GrainMigration);
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Fail to migrate grain {Helper.ConvertGuidToIntString(grainID.id)}, {e.Message}, {e.StackTrace}");
            Debug.Assert(false);
            throw;
        }
    }

    public async Task<GrainMigrationResult> DoMigration(GrainID grainID, int targetPartitionID)
    {
        // can only allow one migration request be handled for each actor
        if (grainToMigrate.Contains(grainID)) throw new SnapperException(ExceptionType.GrainMigration);

        // can only allow migration grain to a different silo
        var masterGrainID = snapperClusterCache.GetMasterGrain(grainID);
        var old_location = masterGrainID.location;
        var new_location = snapperClusterCache.GetLocation(targetPartitionID);
        if (old_location == new_location) throw new SnapperException(ExceptionType.GrainMigration);
 
        grainToMigrate.Add(grainID);
        
        var res = new GrainMigrationResult();
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        // freeze the grain itself and stop accepting more ACT execution
        //Console.WriteLine($"MW: try to freeze grain {grainID.Print()}, target partition = {targetPartitionID}");
        var tasks = new List<Task>();
        
        var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(masterGrainID.grainID.id, masterGrainID.location, masterGrainID.grainID.className);
        tasks.Add(grain.FreezeGrain());
        //await Task.WhenAll(tasks);
        //tasks.Clear();
        //Console.WriteLine($"MW: the grain is feezed now");

        // freeze the grain in all GrainPlacementManager
        foreach (var pm in allPMs) tasks.Add(pm.FreezeGrain(grainID));
        await Task.WhenAll(tasks);
        //Console.WriteLine($"MW: all related PACTs have committed");
        tasks.Clear();
        res.SetTime(GrainMigrationTimeInterval.FreezeGrain, stopWatch.ElapsedMilliseconds);
        stopWatch.Restart();

        // update grain placement info stored on redis
        //Console.WriteLine($"MW: update redis");
        await grainPlacementChange_db.ListRightPushAsync("GrainMigration", grainID.Print() + " || " + old_location + " || " + new_location);
        res.SetTime(GrainMigrationTimeInterval.UpdateRedis, stopWatch.ElapsedMilliseconds);
        stopWatch.Restart();

        // update grain placement info cached on each silo (global, regional, and local)
        //Console.WriteLine($"MW: update cache");
        var mwList = snapperClusterCache.GetOneRandomMigrationWorkerPerSilo();
        foreach (var item in mwList)
        {
            var mw = GrainFactory.GetGrain<IGrainMigrationWorker>(item.Item1, item.Item2, typeof(GrainMigrationWorker).FullName);
            tasks.Add(mw.UpdateUserGrainInfoInCache(grainID, targetPartitionID));
        }
        await Task.WhenAll(tasks);
        tasks.Clear();
        res.SetTime(GrainMigrationTimeInterval.UpdateCache, stopWatch.ElapsedMilliseconds);
        stopWatch.Restart();

        // de-activate the grain
        //Console.WriteLine($"MW: de-activate grain");
        (var prevTimestamp, var prevPreparedVersion, var dictionary, var updateReference, var deleteReference, var list) = await grain.DeactivateGrain();
        res.SetTime(GrainMigrationTimeInterval.DeactivateGrain, stopWatch.ElapsedMilliseconds);
        stopWatch.Restart();

        // activate the new grain
        //Console.WriteLine($"MW: activate grain");
        var newMasterGrainID = snapperClusterCache.GetMasterGrain(grainID);
        Debug.Assert(newMasterGrainID.location == new_location);
        var newGrain = GrainFactory.GetGrain<ITransactionExecutionGrain>(newMasterGrainID.grainID.id, newMasterGrainID.location, newMasterGrainID.grainID.className);
        await newGrain.ActivateGrain(prevTimestamp, prevPreparedVersion, dictionary, updateReference, deleteReference, list);
        res.SetTime(GrainMigrationTimeInterval.ActivateGrain, stopWatch.ElapsedMilliseconds);
        stopWatch.Restart();

        // unfreeze the grain in all GrainPlacementManager
        //Console.WriteLine($"MW: unfreeze grain");
        foreach (var pm in allPMs) tasks.Add(pm.UnFreezeGrain(grainID));
        await Task.WhenAll(tasks);
        grainToMigrate.Remove(grainID);
        res.SetTime(GrainMigrationTimeInterval.UnFreezeGrain, stopWatch.ElapsedMilliseconds);

        return res;
    }

    public Task UpdateUserGrainInfoInCache(GrainID grainID, int targetPartitionID)
    {
        snapperClusterCache.UpdateUserGrainInfoInCache(grainID, targetPartitionID);
        return Task.CompletedTask;
    }
}
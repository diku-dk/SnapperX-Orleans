using Concurrency.Common.ILogging;
using Concurrency.Common.Logging;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions;
using Utilities;

namespace SnapperSiloHost.OrleansStorage;

public class FileTransactionalStateStorageFactory : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
{
    readonly string name;
    readonly ISnapperLogger[] loggers;
    readonly MyTransactionalStateOptions options;
    readonly Random rnd;

    public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
    {
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<MyTransactionalStateOptions>>();
        return ActivatorUtilities.CreateInstance<FileTransactionalStateStorageFactory>(services, name, optionsMonitor.Get(name));
    }

    public FileTransactionalStateStorageFactory(string name, MyTransactionalStateOptions options)
    {
        this.name = name;
        this.options = options;
        rnd = new Random();

        if (!Directory.Exists(Constants.logPath)) Directory.CreateDirectory(Constants.logPath);

        loggers = new ISnapperLogger[Constants.numLogFilesPerPartition * options.numPartitionPerSilo];
        for (int i = 0; i < Constants.numLogFilesPerPartition * options.numPartitionPerSilo; i++)
        {
            loggers[i] = new SnapperLogger();
            loggers[i].Init($"Silo-{options.regionID}-{options.siloID}-OrleansTxn-{i}");
        }
    }

    public void Participate(ISiloLifecycle lifecycle)
        => lifecycle.Subscribe(OptionFormattingUtilities.Name<FileTransactionalStateStorageFactory>(name), options.InitStage, Init);

    Task Init(CancellationToken cancellationToken) => Task.CompletedTask;

    public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainContext context) where TState : class, new()
    {
        var str = context.GrainId.ToString();
        var strs = str.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var partitionKey = strs[strs.Length - 1];    // use grainID (long) as partitionKey
        var logger = loggers[rnd.Next(0, loggers.Length)];
        return ActivatorUtilities.CreateInstance<FileTransactionalStateStorage<TState>>(context.ActivationServices, logger, partitionKey);
    }
}

public class FileTransactionalStateStorage<TState> : ITransactionalStateStorage<TState> where TState : class, new()
{
    KeyEntity key;
    readonly string partitionKey;
    List<KeyValuePair<long, StateEntity>> states;

    readonly ISnapperLogger logger;

    public FileTransactionalStateStorage(ISnapperLogger logger, string partitionKey)
    {
        this.partitionKey = partitionKey;
        this.logger = logger;
    }

    public async Task<TransactionalStorageLoadResponse<TState>> Load()
    {
        try
        {
            await Task.CompletedTask;
            key = new KeyEntity(partitionKey);
            states = new List<KeyValuePair<long, StateEntity>>();

            if (string.IsNullOrEmpty(key.ETag)) return new TransactionalStorageLoadResponse<TState>();
            else
            {
                TState committedState;
                if (key.CommittedSequenceId == 0) committedState = new TState();
                else
                {
                    if (!FindState(key.CommittedSequenceId, out var pos))
                    {
                        var error = $"Storage state corrupted: no record for committed state v{key.CommittedSequenceId}";
                        throw new InvalidOperationException(error);
                    }
                    committedState = states[pos].Value.GetState<TState>();
                }

                var PrepareRecordsToRecover = new List<PendingTransactionState<TState>>();
                for (int i = 0; i < states.Count; i++)
                {
                    var kvp = states[i];

                    // pending states for already committed transactions can be ignored
                    if (kvp.Key <= key.CommittedSequenceId)
                        continue;

                    // upon recovery, local non-committed transactions are considered aborted
                    if (kvp.Value.TransactionManager == null)
                        break;

                    var tm = JsonConvert.DeserializeObject<ParticipantId>(kvp.Value.TransactionManager);

                    PrepareRecordsToRecover.Add(new PendingTransactionState<TState>()
                    {
                        SequenceId = kvp.Key,
                        State = kvp.Value.GetState<TState>(),
                        TimeStamp = kvp.Value.TransactionTimestamp,
                        TransactionId = kvp.Value.TransactionId,
                        TransactionManager = tm
                    });
                }

                // clear the state strings... no longer needed, ok to GC now
                for (int i = 0; i < states.Count; i++) states[i].Value.StateJson = null;

                var metadata = JsonConvert.DeserializeObject<TransactionalStateMetaData>(key.Metadata);
                return new TransactionalStorageLoadResponse<TState>(key.ETag, committedState, key.CommittedSequenceId, metadata, PrepareRecordsToRecover);
            }
        }
        catch (Exception)
        {
            throw;
        }
    }


    public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo, long? abortAfter)
    {
        if (key.ETag != expectedETag) throw new ArgumentException(nameof(expectedETag), "Etag does not match");

        // first, clean up aborted records
        if (abortAfter.HasValue && states.Count != 0)
        {
            while (states.Count > 0 && states[states.Count - 1].Key > abortAfter)
                states.RemoveAt(states.Count - 1);
        }

        // second, persist non-obsolete prepare records
        var obsoleteBefore = commitUpTo.HasValue ? commitUpTo.Value : key.CommittedSequenceId;
        if (statesToPrepare != null)
            foreach (var s in statesToPrepare)
                if (s.SequenceId >= obsoleteBefore)
                {
                    if (FindState(s.SequenceId, out var pos))
                    {
                        // overwrite with new pending state
                        var existing = states[pos].Value;
                        existing.TransactionId = s.TransactionId;
                        existing.TransactionTimestamp = s.TimeStamp;
                        existing.TransactionManager = JsonConvert.SerializeObject(s.TransactionManager);
                        existing.SetState(s.State);
                    }
                    else
                    {
                        var entity = StateEntity.Create(s);
                        states.Insert(pos, new KeyValuePair<long, StateEntity>(s.SequenceId, entity));
                    }
                }

        // third, persist metadata and commit position
        key.Metadata = JsonConvert.SerializeObject(metadata);
        if (commitUpTo.HasValue && commitUpTo.Value > key.CommittedSequenceId) key.CommittedSequenceId = commitUpTo.Value;

        // fourth, remove obsolete records
        if (states.Count > 0 && states[0].Key < obsoleteBefore)
        {
            FindState(obsoleteBefore, out var pos);
            states.RemoveRange(0, pos);
        }

        // persist KeyEntity and StateEntity
        await logger.Write(MessagePackSerializer.Serialize(key));
        await logger.Write(MessagePackSerializer.Serialize(states));
        return key.ETag;
    }

    // find the StateEntity who's Key == sequenceId
    private bool FindState(long sequenceId, out int pos)
    {
        for (var i = 0; i < states.Count; i++)
        {
            if (states[i].Key == sequenceId)
            {
                pos = i;
                return true;
            }
        }

        pos = states.Count;
        return false;
    }
}
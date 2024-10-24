﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions;

namespace SnapperSiloHost.OrleansStorage;

public class MemoryTransactionalStateStorageFactory : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly string name;
    private readonly MyTransactionalStateOptions options;

    public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
    {
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<MyTransactionalStateOptions>>();
        return ActivatorUtilities.CreateInstance<MemoryTransactionalStateStorageFactory>(services, name, optionsMonitor.Get(name));
    }

    public MemoryTransactionalStateStorageFactory(string name, MyTransactionalStateOptions options)
    {
        this.name = name;
        this.options = options;
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(OptionFormattingUtilities.Name<MemoryTransactionalStateStorageFactory>(name), options.InitStage, Init);
    }

    private Task Init(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainContext context) where TState : class, new()
    {
        return ActivatorUtilities.CreateInstance<MemoryTransactionalStateStorage<TState>>(context.ActivationServices);
    }
}

public class MemoryTransactionalStateStorage<TState> : ITransactionalStateStorage<TState> where TState : class, new()
{
    private KeyEntity key;
    private List<KeyValuePair<long, StateEntity>> states;

    public MemoryTransactionalStateStorage()
    {
    }

    public Task<TransactionalStorageLoadResponse<TState>> Load()
    {
        try
        {
            if (key == null) key = new KeyEntity("default");
            if (states == null) states = new List<KeyValuePair<long, StateEntity>>();

            if (string.IsNullOrEmpty(key.ETag)) return Task.FromResult(new TransactionalStorageLoadResponse<TState>());
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
                    if (kvp.Key <= key.CommittedSequenceId) continue;

                    // upon recovery, local non-committed transactions are considered aborted
                    if (kvp.Value.TransactionManager == null) break;

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
                return Task.FromResult(new TransactionalStorageLoadResponse<TState>(key.ETag, committedState, key.CommittedSequenceId, metadata, PrepareRecordsToRecover));
            }
        }
        catch (Exception)
        {
            throw;
        }
    }


    public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo, long? abortAfter)
    {
        /*
        //Console.WriteLine($"MemoryStorage: Store()");
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

        return key.ETag;*/
        return "";
    }

    // find the StateEntity who's Key == sequenceId
    private bool FindState(long sequenceId, out int pos)
    {
        pos = 0;
        while (pos < states.Count)
        {
            switch (states[pos].Key.CompareTo(sequenceId))
            {
                case 0:
                    return true;
                case -1:
                    pos++;
                    continue;
                case 1:
                    return false;
            }
        }
        return false;
    }
}
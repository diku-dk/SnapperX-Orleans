using Concurrency.Common;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution;

/// <summary> 
/// SnapperReadWriteLock provides a read / write lock that
/// one transaction can only get the lock for once (no matter for read or write lock)
/// integrated with PACT concurrency control
/// </summary>
internal class SnapperReadWriteLock
{
    /// <summary> isDet, tid, read / write, task </summary>
    Queue<(bool, SnapperID, AccessMode, TaskCompletionSource)> waitinglist;
    
    /// <summary> reader transactions that hold the lock now, bool: isDet </summary>
    Dictionary<SnapperID, bool> concurrentReaders;

    /// <summary> writer transaction that holds the lock now, bool: isDet </summary>
    (SnapperID, bool) writer;

    public SnapperReadWriteLock()
    {
        waitinglist = new Queue<(bool, SnapperID, AccessMode, TaskCompletionSource)>();
        concurrentReaders = new Dictionary<SnapperID, bool>();
        writer = (new SnapperID(), true);
    }

    public void CheckGC()
    {
        if (!writer.Item1.isEmpty()) Console.WriteLine($"SnapperReadWriteLock: writer is not empty");
        if (waitinglist.Count != 0) Console.WriteLine($"SnapperReadWriteLock: waitinglist.Count = {waitinglist.Count}");
        if (concurrentReaders.Count != 0) Console.WriteLine($"SnapperReadWriteLock: concurrentReaders.Count = {concurrentReaders.Count}");
    }

    public async Task AcquireLock(AccessMode mode, TransactionContext cxt)
    {
        switch (mode)
        {
            case AccessMode.Read:
                await GetReadLock(cxt);
                break;
            case AccessMode.ReadWrite:
                await GetWriteLock(cxt);
                break;
        }
    }

    async Task GetReadLock(TransactionContext cxt)
    {
        // =========================================================================================================
        // if nobody is reading or writing the grain
        if (concurrentReaders.Count == 0 && writer.Item1.isEmpty())
        {
            concurrentReaders.Add(cxt.tid, cxt.isDet);
            return;
        }

        // =========================================================================================================
        // if a writer is holding the lock now
        if (!writer.Item1.isEmpty())
        {
            Debug.Assert(concurrentReaders.Count == 0);
            Debug.Assert(!writer.Item1.Equals(cxt.tid));

            // if the current one is an ACT, the writer is also an ACT, the current one should abort according to wait-die rule to avoid deadlock between ACTs
            if (!cxt.isDet && !writer.Item2 && cxt.tid.CompareTo(writer.Item1) > 0) throw new SnapperException(ExceptionType.RWConflict);
           
            // enqueue and wait
            var myLock = new TaskCompletionSource();
            waitinglist.Enqueue((cxt.isDet, cxt.tid, AccessMode.Read, myLock));
            await myLock.Task;
            return;
        }

        // =========================================================================================================
        // if one or multiple readers are holding the lock now
        if (concurrentReaders.Count != 0)
        {
            Debug.Assert(writer.Item1.isEmpty());
            Debug.Assert(!concurrentReaders.ContainsKey(cxt.tid));

            concurrentReaders.Add(cxt.tid, cxt.isDet);
        }
    }

    async Task GetWriteLock(TransactionContext cxt)
    {
        var isDet = !cxt.bid.isEmpty();

        // =========================================================================================================
        // if nobody is reading or writing the grain
        if (concurrentReaders.Count == 0 && writer.Item1.isEmpty())
        {
            writer = (cxt.tid, isDet);
            return;
        }

        // =========================================================================================================
        // if a writer is holding the lock now
        if (!writer.Item1.isEmpty())
        {
            Debug.Assert(concurrentReaders.Count == 0);
            Debug.Assert(!writer.Item1.Equals(cxt.tid));

            // if the current one is an ACT, the writer is also an ACT, the current one should abort according to wait-die rule to avoid deadlock between ACTs
            if (!isDet && !writer.Item2 && cxt.tid.CompareTo(writer.Item1) > 0) throw new SnapperException(ExceptionType.RWConflict);

            // enqueue and wait
            var myLock = new TaskCompletionSource();
            waitinglist.Enqueue((isDet, cxt.tid, AccessMode.ReadWrite, myLock));
            await myLock.Task;
            return;
        }

        // =========================================================================================================
        // if one or multiple readers are holding the lock now
        if (concurrentReaders.Count != 0)
        {
            Debug.Assert(writer.Item1.isEmpty());
            Debug.Assert(!concurrentReaders.ContainsKey(cxt.tid));

            // if the current one is an ACT
            if (!isDet)
            {
                // if any reader is also an ACT, the current one should abort according to wait-die rule to avoid deadlock between ACTs
                foreach (var reader in concurrentReaders)
                    if (!reader.Value && cxt.tid.CompareTo(reader.Key) > 0) throw new SnapperException(ExceptionType.RWConflict);
            }
            
            // enqueue and wait
            var myLock = new TaskCompletionSource();
            waitinglist.Enqueue((isDet, cxt.tid, AccessMode.ReadWrite, myLock));
            await myLock.Task;
        }
    }

    public void ReleaseLock(SnapperID tid)
    {
        // if tid is a writer
        if (writer.Item1.Equals(tid))
        {
            Debug.Assert(concurrentReaders.Count == 0);
            writer = (new SnapperID(), true);
        } 
        else
        {
            if (!concurrentReaders.ContainsKey(tid)) return;
            Debug.Assert(writer.Item1.isEmpty());

            // if tid is a reader
            concurrentReaders.Remove(tid);
            if (concurrentReaders.Count != 0) return;
        }

        if (waitinglist.Count == 0) return;

        // unblcok the first txn in the queue
        var txn = waitinglist.Dequeue();
        if (txn.Item3 == AccessMode.ReadWrite)
        {
            writer = (txn.Item2, txn.Item1);
            txn.Item4.SetResult();
            return;
        }
        else
        {
            var promises = new List<TaskCompletionSource>();

            concurrentReaders.Add(txn.Item2, txn.Item1);
            promises.Add(txn.Item4);

            // if the unblocked txn is a reader, can unblocked more subsequent readers
            while (waitinglist.Count != 0)
            {
                var txn1 = waitinglist.Peek();
                if (txn1.Item3 == AccessMode.ReadWrite) break;

                concurrentReaders.Add(txn1.Item2, txn1.Item1);
                waitinglist.Dequeue();
                promises.Add(txn.Item4);
            }

            Debug.Assert(writer.Item1.isEmpty());

            promises.ForEach(x => x.SetResult());
        }
    }

    /// <returns> if the given transaction is the only one that currently holds the lock and no other transactions are qaiting for the lock </returns>
    public bool IsTheOnlyOneHoldLock(SnapperID tid)
    {
        // the transaction currently holds the write lock
        if (writer.Item1.Equals(tid))
        {
            Debug.Assert(concurrentReaders.Count == 0);
            return waitinglist.Count == 0;
        }

        // the transaction currently holds the read lock
        if (concurrentReaders.ContainsKey(tid)) return concurrentReaders.Count == 1 && waitinglist.Count == 0;

        return false;
    }

    /// <returns> if no transaction is holding or waiting for the lock </returns>
    public bool IsEmpty() => writer.Item1.isEmpty() && concurrentReaders.Count == 0 && waitinglist.Count == 0;
}
using Concurrency.Common;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution;

internal class SnapperReadWriteLockNoWait
{
    readonly SnapperGrainID myID;

    /// <summary> the writer that currently holds the lock </summary>
    SnapperID writer;

    /// <summary> the readers that currently together hold the read lock </summary>
    HashSet<SnapperID> concurrentReaders;

    public SnapperReadWriteLockNoWait(SnapperGrainID myID)
    {
        this.myID = myID;
        writer = new SnapperID();
        concurrentReaders = new HashSet<SnapperID>();
    }

    public void CheckGC()
    {
        if (!writer.isEmpty()) Console.WriteLine($"SnapperReadWriteLockNoWait: writer is not empty. ");
        if (concurrentReaders.Count != 0) Console.WriteLine($"SnapperReadWriteLockNoWait: concurrentReaders.Count = {concurrentReaders.Count}");
    }

    public async Task AcquireLock(AccessMode mode, TransactionContext cxt)
    {
        switch (mode)
        {
            case AccessMode.Read: GetReadLock(cxt); break;
            case AccessMode.ReadWrite: GetWriteLock(cxt); break;
        }

        await Task.CompletedTask;
    }

    void GetReadLock(TransactionContext cxt)
    {
        if (!writer.isEmpty()) throw new SnapperException(ExceptionType.RWConflict);

        concurrentReaders.Add(cxt.tid);
    }

    void GetWriteLock(TransactionContext cxt)
    {
        if (!writer.isEmpty() || concurrentReaders.Count != 0) throw new SnapperException(ExceptionType.RWConflict);

        writer = cxt.tid;
    }

    public void ReleaseLock(SnapperID tid)
    {
        if (writer.Equals(tid))
        {
            Debug.Assert(concurrentReaders.Count == 0);
            writer = new SnapperID();
        }
        else if (concurrentReaders.Contains(tid))
        {
            Debug.Assert(writer.isEmpty());
            concurrentReaders.Remove(tid);
        }
    }

    public bool IsTheOnlyOneHoldLock(SnapperID tid)
    {
        if (writer.Equals(tid))
        {
            Debug.Assert(concurrentReaders.Count == 0);
            return true;
        }
        else if (concurrentReaders.Contains(tid))
        {
            Debug.Assert(writer.isEmpty());
            return concurrentReaders.Count == 1;
        }

        return false;
    }

    public bool IsEmpty() => writer.isEmpty() && concurrentReaders.Count == 0;

}
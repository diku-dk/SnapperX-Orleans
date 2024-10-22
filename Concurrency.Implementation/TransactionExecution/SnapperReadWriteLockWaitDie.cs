using Concurrency.Common;
using System.Diagnostics;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution;

/// <summary> the lock support read/write lock + wait-die </summary>
internal class SnapperReadWriteLockWaitDie
{
    readonly SnapperGrainID myID;

    /// <summary> sort by descending order of tid </summary>
    SortedDictionary<SnapperID, (bool, TaskCompletionSource)> waitinglist;   // bool: isReader

    /// <summary> 
    /// sort by ascending order of tid 
    /// minWorkReader used to decide if a writer can be added after concurrent working readers
    /// </summary>
    SortedSet<SnapperID> concurrentReaders;

    /// <summary> decide if a reader can be added before waiting writers </summary>
    SnapperID maxWaitWriter;   

    public SnapperReadWriteLockWaitDie(SnapperGrainID myID)
    {
        this.myID = myID;
        var descendingComparer = Comparer<SnapperID>.Create((x, y) => y.CompareTo(x));
        waitinglist = new SortedDictionary<SnapperID, (bool, TaskCompletionSource)>(descendingComparer);
        concurrentReaders = new SortedSet<SnapperID>();
        maxWaitWriter = new SnapperID();
    }

    public void CheckGC()
    {
        if (!maxWaitWriter.isEmpty()) Console.WriteLine($"SnapperReadWriteLock: the maxWaitWriter is not empty");
        if (waitinglist.Count != 0) Console.WriteLine($"SnapperReadWriteLock: waitinglist.Count = {waitinglist.Count}");
        if (concurrentReaders.Count != 0) Console.WriteLine($"SnapperReadWriteLock: concurrentReaders.Count = {concurrentReaders.Count}");
    }

    public async Task AcquireLock(AccessMode mode, TransactionContext cxt)
    {
        switch (mode)
        {
            case AccessMode.Read:
                await GetReadLock(cxt.tid);
                break;
            case AccessMode.ReadWrite:
                await GetWriteLock(cxt.tid);
                break;
        }
    }

    async Task GetReadLock(SnapperID tid)
    {
        // =========================================================================================================
        // if nobody is reading or writing
        if (waitinglist.Count == 0)
        {
            var mylock = new TaskCompletionSource();
            mylock.SetResult();
            waitinglist.Add(tid, (true, mylock));
            Debug.Assert(concurrentReaders.Count == 0);
            concurrentReaders.Add(tid);
            return;
        }

        // =========================================================================================================
        // if there are multiple readers reading the grain now
        if (concurrentReaders.Count > 0)
        {
            // tid wants to read again
            if (waitinglist.ContainsKey(tid)) 
            {
                // tid must be a reader
                Debug.Assert(waitinglist[tid].Item1 && waitinglist[tid].Item2.Task.IsCompleted);
                return;
            }

            // tid is the first time to read
            var mylock = new TaskCompletionSource();
            if (tid.CompareTo(maxWaitWriter) > 0)    // check if this reader can be put in front of the waiting writers
            {
                mylock.SetResult();
                waitinglist.Add(tid, (true, mylock));
                concurrentReaders.Add(tid);
                return;
            }

            // otherwise, the reader need to be added after the waiting writer
            waitinglist.Add(tid, (true, mylock));
            await mylock.Task;
            return;
        }

        // =========================================================================================================
        // otherwise, right now there is only one writer working
        Debug.Assert(!waitinglist.First().Value.Item1 && waitinglist.First().Value.Item2.Task.IsCompleted);
        if (waitinglist.ContainsKey(tid))    // tid has been added as a writer before
        {
            if (tid != waitinglist.First().Key) throw new SnapperException(ExceptionType.NotSerializable);
            //Debug.Assert(tid == waitinglist.First().Key);
            return;
        }
        if (tid.CompareTo(waitinglist.First().Key) < 0)
        {
            var mylock = new TaskCompletionSource();
            waitinglist.Add(tid, (true, mylock));
            await mylock.Task;
            return;
        }
        throw new SnapperException(ExceptionType.RWConflict);
    }

    async Task GetWriteLock(SnapperID tid)
    {
        // =========================================================================================================
        // if nobody is reading or writing the grain
        if (waitinglist.Count == 0)    
        {
            var mylock = new TaskCompletionSource();
            mylock.SetResult();
            waitinglist.Add(tid, (false, mylock));
            return;
        }

        // =========================================================================================================
        if (waitinglist.ContainsKey(tid))
        {
            await waitinglist[tid].Item2.Task;
            //Debug.Assert(waitinglist[tid].Item2.Task.IsCompleted);  // tid must be reading or writing the grain right now
            if (waitinglist[tid].Item1)                             // if tid has been added as a reader before
            {
                Debug.Assert(concurrentReaders.Count > 0);          // right now there must be readers working
                throw new SnapperException(ExceptionType.RWConflict);
            }
            return;  // if tid has been added as a writer before, this writer must be working now
        }

        // =========================================================================================================
        if (concurrentReaders.Count > 0)  // right now there are multiple readers reading
        {
            if (tid.CompareTo(concurrentReaders.Min) < 0)
            {
                var mylock = new TaskCompletionSource();
                waitinglist.Add(tid, (false, mylock));
                maxWaitWriter = maxWaitWriter.CompareTo(tid) > 0 ? maxWaitWriter : tid;
                await mylock.Task;
                return;
            }
            throw new SnapperException(ExceptionType.RWConflict);
        }

        // =========================================================================================================
        // otherwise, if there is a writer working
        if (tid.CompareTo(waitinglist.First().Key) < 0)
        {
            var mylock = new TaskCompletionSource();
            waitinglist.Add(tid, (false, mylock));
            await mylock.Task;
            return;
        }
        throw new SnapperException(ExceptionType.RWConflict);
    }

    public void ReleaseLock(SnapperID tid)
    {
        if (!waitinglist.ContainsKey(tid)) return;   // which means tid has been aborted before do any read write on this grain
        var isReader = waitinglist[tid].Item1;
        waitinglist.Remove(tid);
        //Console.WriteLine($"grain {myID.grainID.Print()}: release lock for tid = {tid.Print()}, isReader = {isReader}");

        if (isReader)
        {
            concurrentReaders.Remove(tid);
            if (concurrentReaders.Count == 0) Debug.Assert(waitinglist.Count == 0 || !waitinglist.First().Value.Item1);
        }

        if (concurrentReaders.Count == 0)
        {
            maxWaitWriter = new SnapperID();
            if (waitinglist.Count == 0) return;
            if (waitinglist.First().Value.Item1)   // if next waiting transaction is a reader
            {
                Debug.Assert(!isReader);           // tid must be a writer
                var tasks = new List<TaskCompletionSource>();
                foreach (var txn in waitinglist)   // there may be multiple readers waiting
                {
                    if (!txn.Value.Item1) break;
                    else
                    {
                        concurrentReaders.Add(txn.Key);
                        tasks.Add(txn.Value.Item2);
                    }
                }
                for (int i = 0; i < tasks.Count; i++) tasks[i].SetResult();
            }
            else waitinglist.First().Value.Item2.SetResult();
        }
    }

    /// <returns> if the given transaction is the only one that currently holds the lock and no other transactions are qaiting for the lock </returns>
    public bool IsTheOnlyOneHoldLock(SnapperID tid)
    {
        // the transaction does not hold the lock
        if (!waitinglist.ContainsKey(tid)) return false;

        // the transaction currently holds the write lock
        if (waitinglist.First().Key.Equals(tid) && !waitinglist.First().Value.Item1)
        {
            Debug.Assert(concurrentReaders.Count == 0);
            return waitinglist.Count == 1;
        }

        // the transaction currently holds the read lock
        if (concurrentReaders.Contains(tid))
        {
            if (waitinglist.Count == 1) Debug.Assert(concurrentReaders.Count == 1);
            return waitinglist.Count == 1;
        }

        return false;
    }

    /// <returns> if no transaction is holding or waiting for the lock </returns>
    public bool IsEmpty() => waitinglist.Count == 0;
}
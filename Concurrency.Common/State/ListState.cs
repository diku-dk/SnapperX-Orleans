using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.State;

public class ListState : IListState
{
    readonly AccessMode accessMode;

    List<SnapperRecord> logs;

    /// <summary> keep record of the newly appened records to the list </summary>
    List<SnapperRecord> newLogs;

    public ListState(AccessMode accessMode, List<SnapperRecord> existingLogs)
    {
        Debug.Assert(accessMode != AccessMode.NoOp);
        this.accessMode = accessMode;
        logs = existingLogs;
        newLogs = new List<SnapperRecord>();
    }

    public List<SnapperRecord> GetUpdates() => newLogs;

    public int CurrentCount() => logs.Count();

    // ========================================================================================== interfaces that are accessible for users ==========
    public List<SnapperRecord> GetAllEntries() => logs;

    public void Add(ISnapperValue value)
    {
        if (accessMode != AccessMode.ReadWrite) throw new Exception("Illegal access to grain state. ");

        var newRecord = new SnapperRecord(value);
        newLogs.Add(newRecord);
        logs.Add(newRecord);
    }
}
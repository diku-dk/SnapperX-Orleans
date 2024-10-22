namespace Concurrency.Common.State;

public interface IListState
{
    /// <summary> get all exisiting entries in the list </summary>
    public List<SnapperRecord> GetAllEntries();

    /// <summary> append a new entry to the list </summary>
    public void Add(ISnapperValue value);
}
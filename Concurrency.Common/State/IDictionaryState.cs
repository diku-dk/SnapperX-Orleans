namespace Concurrency.Common.State;

public interface IDictionaryState
{
    /// <summary> retrieve a specific key </summary>
    /// <returns> the found value or null if not found </returns>
    public ISnapperValue? Get(ISnapperKey key);

    /// <summary> get all exisiting kv pairs </summary>
    public Dictionary<ISnapperKey, SnapperValue> GetAllItems();

    /// <summary> add a new kv pair or update exisiting pair </summary>
    /// <returns> if its added or updated successfully, and the reason </returns>
    public (bool, string) Put(ISnapperKey key, ISnapperValue value);

    /// <summary> remove a specified key </summary>
    /// <returns> if the key is successfully removed, and the reason </returns>
    public (bool, string) Delete(ISnapperKey key);

    // for reference
    public (bool, string) PutKeyWithReference(ReferenceInfo info, ISnapperValue value);

    public (bool, ISnapperValue?) RegisterReference(ReferenceInfo info);
}
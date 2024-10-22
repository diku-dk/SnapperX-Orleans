using System.Diagnostics;

namespace Concurrency.Common.State;

public class Update
{
    public readonly bool isForwarded;

    public readonly UpdateType updateType;

    public readonly ISnapperKey key;

    public readonly SnapperValue value;

    public readonly ISnapperValue? oldValue;    // this value is only set when updateType is Modify

    public Update(bool isForwarded, UpdateType updateType, ISnapperKey key, SnapperValue value, ISnapperValue? oldValue = null)
    {
        this.isForwarded = isForwarded;
        this.updateType = updateType;
        this.key = key;
        this.value = value;

        if (updateType == UpdateType.Modify) Debug.Assert(oldValue != null);
        this.oldValue = oldValue;
    }
}
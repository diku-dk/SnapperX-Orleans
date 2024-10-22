using Utilities;

namespace Concurrency.Common.State;

public class ForwardedUpdate
{
    /// <summary> mark if this update is forwarded to a follower grain or a master grain </summary>
    public readonly bool toFollower;

    public readonly Update update;

    public readonly SnapperKeyReferenceType referenceType;

    public readonly GrainID affectedGrain;

    public readonly ISnapperKey affectedKey;

    public readonly IUpdateFunction? function;

    public ForwardedUpdate(bool toFollower, Update update, SnapperKeyReferenceType referenceType, GrainID affectedGrain, ISnapperKey affectedKey, IUpdateFunction? function)
    {
        this.toFollower = toFollower;
        this.update = update;
        this.referenceType = referenceType;
        this.affectedGrain = affectedGrain;
        this.affectedKey = affectedKey;
        this.function = function;
    }
}
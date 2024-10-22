using System.Diagnostics;
using Utilities;

namespace Concurrency.Common.State;

public class SnapperValue : IEquatable<SnapperValue>, ICloneable
{
    public readonly ISnapperValue value;

    public readonly string keyTypeName;

    public readonly SnapperKeyReferenceType referenceType;

    // the following properties are set only if this key is referring to (or depend on) another key

    public readonly IUpdateFunction? function;

    public readonly GrainID? dependentGrain;

    public readonly ISnapperKey? dependentKey;

    public SnapperValue(ISnapperValue value, string keyTypeName, SnapperKeyReferenceType referenceType, IUpdateFunction? function = null, GrainID? dependentGrain = null, ISnapperKey? dependentKey = null)
    {
        this.value = value;

        Debug.Assert(!string.IsNullOrEmpty(keyTypeName));
        this.keyTypeName = keyTypeName;

        this.referenceType = referenceType;

        if (referenceType != SnapperKeyReferenceType.NoReference)
        {
            Debug.Assert(function != null && dependentGrain != null && dependentKey != null);
            this.function = function;
            this.dependentGrain = dependentGrain;
            this.dependentKey = dependentKey;
        }
        else Debug.Assert(dependentGrain == null && dependentKey == null && function == null);
    }

    public bool Equals(SnapperValue? other)
    {
        if (other == null) return false;

        if (!other.value.Equals(value)) return false;
        if (!other.keyTypeName.Equals(keyTypeName)) return false;
        
        if (!other.referenceType.Equals(referenceType)) return false; 
        
        if (other.dependentGrain == null)
        {
            Debug.Assert(other.dependentKey == null && other.function == null);
            if (dependentGrain != null || dependentKey != null || function != null) return false;
        }
        else
        {
            Debug.Assert(other.dependentKey != null && other.function != null);
            if (!other.dependentGrain.Equals(dependentGrain) || !other.dependentKey.Equals(dependentKey) || !other.function.Equals(function)) return false;
        }

        return true;
    }

    public object Clone()
    {
        var valueCopy = value.Clone() as ISnapperValue;
        Debug.Assert(valueCopy != null);
        var keyTypeNameCopy = keyTypeName.Clone() as string;
        Debug.Assert(!string.IsNullOrEmpty(keyTypeNameCopy));
        var functionCopy = function == null ? null : function.Clone() as IUpdateFunction;
        var dependentGrainCopy = dependentGrain == null ? null : dependentGrain.Clone() as GrainID;
        var dependentKeyCopy = dependentKey == null ? null : dependentKey.Clone() as ISnapperKey;
        return new SnapperValue(valueCopy, keyTypeNameCopy, referenceType, functionCopy, dependentGrainCopy, dependentKeyCopy);
    }
}
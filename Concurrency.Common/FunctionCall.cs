using MessagePack;
using System.Diagnostics;
using System.Reflection;

namespace Concurrency.Common;

[MessagePackObject]
[GenerateSerializer]
public class FunctionCall
{
    [Id(0)]
    [Key(0)]
    public readonly string funcName;

    [Id(1)]
    [Key(1)]
    public readonly Type? type;

    [Id(2)]
    [Key(2)]
    public readonly object? funcInput;

    public FunctionCall(MethodInfo func, object? funcInput = null)
    {
        funcName = func.Name;
        Debug.Assert(func.ReflectedType != null);
        type = func.ReflectedType;
        this.funcInput = funcInput;
    }

    /// <summary> only used for internal function calls, such as "AddKeyReference" and "AddCopiedKey" </summary>
    public FunctionCall(string funcName, object? funcInput = null)
    {
        this.funcName = funcName;
        type = null;
        this.funcInput = funcInput;
    }
}
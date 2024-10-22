using Utilities;

namespace Concurrency.Common;

[GenerateSerializer]
public class SnapperException : Exception
{
    [Id(0)]
    public readonly ExceptionType exceptionType;

    public SnapperException(ExceptionType exceptionType) : base() { this.exceptionType = exceptionType; }
}
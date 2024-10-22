namespace Concurrency.Common.ILogging;

public interface ISnapperLogger
{
    void CleanUp();

    Task Init(string name);

    Task Write(byte[] value);
}
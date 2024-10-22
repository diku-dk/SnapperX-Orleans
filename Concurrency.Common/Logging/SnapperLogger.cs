using Utilities;
using Concurrency.Common.ILogging;
using System.Diagnostics;

namespace Concurrency.Common.Logging;

public class SnapperLogger : ISnapperLogger
{
    string fileName;
    FileStream fileWriter;
    SemaphoreSlim fileLock;

    public void CleanUp()
    {
        if (fileWriter == null) return;
        if (File.Exists(fileName))
        {
            File.Delete(fileName);
            Console.WriteLine($"delete file {fileName}");
        }
    }

    public async Task Init(string name)
    {
        try
        {
            fileLock = new SemaphoreSlim(1);
            fileName = Constants.logPath + name;

            await fileLock.WaitAsync();
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
                Console.WriteLine($"delete file {fileName}");
            }
            fileWriter = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read);
            fileLock.Release();
        }
        catch (Exception e)
        {
            Console.WriteLine($"{e.Message}, {e.StackTrace}");
            Debug.Assert(false);
        }
    }

    async Task ISnapperLogger.Write(byte[] value)
    {
        await fileLock.WaitAsync();
        var sizeBytes = BitConverter.GetBytes(value.Length);
        await fileWriter.WriteAsync(sizeBytes, 0, sizeBytes.Length);
        await fileWriter.WriteAsync(value, 0, value.Length);
        await fileWriter.FlushAsync();
        fileLock.Release();
    }
}
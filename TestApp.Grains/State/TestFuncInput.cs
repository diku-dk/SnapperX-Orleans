using Concurrency.Common;
using Utilities;

namespace TestApp.Grains.State;

public enum OperationType { Read, Update, Delete };

[GenerateSerializer]
public class TestFuncInput
{
    [Id(0)]
    public Dictionary<GrainID, (AccessMode, OperationType, TestFuncInput)> info;    

    public TestFuncInput(Dictionary<GrainID, (AccessMode, OperationType, TestFuncInput)> info) => this.info = info;

    public TestFuncInput() => info = new Dictionary<GrainID, (AccessMode, OperationType, TestFuncInput)>();
}
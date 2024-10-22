using Concurrency.Common.State;
using MarketPlace.Grains.ValueState;
using MessagePack;
using System.Diagnostics;

namespace MarketPlace.Grains.Grains.SnapperTransactionalFineGrains;

[MessagePackObject]
[GenerateSerializer]
public class SellerDashBoardFunction : IUpdateFunction
{
    public SellerDashBoardFunction() { }

    public ISnapperValue ApplyUpdate(ISnapperKey key1, ISnapperValue oldValue, ISnapperValue newValue, ISnapperKey key2, ISnapperValue value2)
    {
        Debug.Assert(key1.Equals(key2));

        var lastCreatedPackage = newValue as PackageInfo;
        Debug.Assert(lastCreatedPackage != null);

        var sellerInfo = value2 as SellerInfo;
        Debug.Assert(sellerInfo != null);

        sellerInfo.totalNumOrder.Add(1);
        sellerInfo.totalNumProduct.Add(lastCreatedPackage.items.Count);

        return sellerInfo;
    }

    public byte[] Serialize(IUpdateFunction data)
    {
        var func = data as SellerDashBoardFunction;
        if (func == null) throw new Exception($"The input function is not with type SellerDashBoardFunction");

        return MessagePackSerializer.Serialize(func);
    }

    public IUpdateFunction Deserialize(byte[] bytes) => MessagePackSerializer.Deserialize<SellerDashBoardFunction>(bytes);

    public string? GetAssemblyQualifiedName() => typeof(SellerDashBoardFunction).AssemblyQualifiedName;

    public bool Equals(IUpdateFunction? other)
    {
        if (other == null) return false;
        var func = other as SellerDashBoardFunction;
        return func != null;
    }

    public object Clone() => new SellerDashBoardFunction();
}
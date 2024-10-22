using Concurrency.Common.State;
using MessagePack;

namespace MarketPlace.Grains.ValueState;

[GenerateSerializer]
[MessagePackObject]
public class ShipmentInfo : AbstractValue<ShipmentInfo>, ISnapperValue
{
    [Key(0)]
    [Id(0)]
    public DateTime lastUpdatedTime;

    [Key(1)]
    [Id(1)]
    public string detail;

    public ShipmentInfo() { lastUpdatedTime = DateTime.MinValue; detail = ""; }

    public ShipmentInfo(DateTime lastUpdatedTime, string detail) { this.lastUpdatedTime = lastUpdatedTime; this.detail = detail; }

    public object Clone() => new ShipmentInfo(lastUpdatedTime, (string)detail.Clone());

    public bool Equals(ISnapperValue? other)
    {
        if (other == null) return false;

        try
        {
            var data = other as ShipmentInfo;
            if (data == null) return false;
            return lastUpdatedTime.Equals(data.lastUpdatedTime) && detail == data.detail;
        }
        catch { return false; }
    }

    public string Print() => $"ShipmentInfo: lastUpdatedTime = {lastUpdatedTime}, detail = {detail}";
}
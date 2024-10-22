using Confluent.Kafka;
using MessagePack;

namespace Concurrency.Common.Logging;

public class SnapperLogSerializer : ISerializer<SnapperLog>, IDeserializer<SnapperLog>
{
    public byte[] Serialize(SnapperLog log, SerializationContext _) => MessagePackSerializer.Serialize(log);

    public SnapperLog Deserialize(ReadOnlySpan<byte> bytes, bool isNull, SerializationContext _)
    {
        if (isNull) return null;
        return MessagePackSerializer.Deserialize<SnapperLog>(bytes.ToArray());
    }
}
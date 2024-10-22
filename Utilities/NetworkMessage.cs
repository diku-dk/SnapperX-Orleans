using MessagePack;

namespace Utilities;

[MessagePackObject]
public class NetworkMessage
{
    [Key(0)]
    public NetMsgType msgType;

    [Key(1)]
    public byte[] content;

    public NetworkMessage(NetMsgType msgType)
    {
        this.msgType = msgType;
    }

    public NetworkMessage(NetMsgType msgType, byte[] content)
    {
        this.msgType = msgType;
        this.content = content;
    }
}
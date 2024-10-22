using MessagePack;
using Newtonsoft.Json;
using Orleans.Transactions.Abstractions;

namespace SnapperSiloHost.OrleansStorage;

[MessagePackObject]
public class KeyEntity
{
    [Key(0)]
    public string ETag;
    [Key(1)]
    public long CommittedSequenceId { get; set; }
    [Key(2)]
    public string Metadata { get; set; }

    public KeyEntity(string partitionKey)
    {
        ETag = partitionKey;
        CommittedSequenceId = 0;
        Metadata = JsonConvert.SerializeObject(new TransactionalStateMetaData());
    }
}
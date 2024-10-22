namespace Concurrency.Common;

[GenerateSerializer]
public class TransactionContext
{
    [Id(0)]
    public SnapperID bid;

    /// <summary> the globally unique transaction identifier </summary>
    [Id(1)]
    public readonly SnapperID tid;

    [Id(2)]
    public readonly bool isDet;

    /// <summary> for PACT </summary>
    public TransactionContext(SnapperID bid, SnapperID tid)
    {
        this.bid = bid;
        this.tid = tid;
        isDet = true;
    }

    /// <summary> for ACT </summary>
    public TransactionContext(SnapperID tid)
    {
        bid = new SnapperID();
        this.tid = tid;
        isDet = false;
    }
}
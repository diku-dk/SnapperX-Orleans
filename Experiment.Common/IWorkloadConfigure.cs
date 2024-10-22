namespace Experiment.Common;

public interface IWorkloadConfigure
{
    public int GetActPipeSize();

    public int GetPactPipeSize();

    public int GetNumActConsumer();

    public int GetNumPactConsumer();

    public int GetMigrationPipeSize();

    public int GetNumMigrationConsumer();
}
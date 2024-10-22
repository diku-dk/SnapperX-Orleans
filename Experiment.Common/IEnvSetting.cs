namespace Experiment.Common;

public interface IEnvSetting
{
    public BasicEnvSetting GetBasic();

    /// <summary> print to console </summary>
    public void PrintContent();

    public int GetTotalNumMasterPartitions();

    /// <summary> print to file </summary>
    public void PrintContent(StreamWriter file);

    public int GetNumPartitionPerSilo();
}
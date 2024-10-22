namespace Experiment.Accelerator;

internal class BasicConfiguration
{
    public readonly int experimentID;

    public readonly int experimentIndex1;

    public readonly int experimentIndex2;

    public readonly int numRegion;

    public readonly int numSiloPerRegion;

    public readonly int numReplicaSiloPerRegion;

    public BasicConfiguration(
        int experimentID,
        int experimentIndex1,
        int experimentIndex2,
        int numRegion,
        int numSiloPerRegion,
        int numReplicaSiloPerRegion)
    {
        this.experimentID = experimentID;
        this.experimentIndex1 = experimentIndex1;
        this.experimentIndex2 = experimentIndex2;
        this.numRegion = numRegion;
        this.numSiloPerRegion = numSiloPerRegion;
        this.numReplicaSiloPerRegion = numReplicaSiloPerRegion;
    }
}
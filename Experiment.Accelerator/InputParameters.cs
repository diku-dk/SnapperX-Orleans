namespace Experiment.Accelerator;

internal class InputParameters
{
    public readonly string redis_ConnectionString;

    public readonly string kafka_ConnectionString;

    public readonly string controller_PublicIP;

    public readonly int instanceIndex;

    public InputParameters(
        string redis_ConnectionString, 
        string kafka_ConnectionString, 
        string controller_PublicIP,
        int instanceIndex)
    { 
        this.redis_ConnectionString = redis_ConnectionString;
        this.kafka_ConnectionString = kafka_ConnectionString;
        this.controller_PublicIP = controller_PublicIP;
        this.instanceIndex = instanceIndex;
    }
}
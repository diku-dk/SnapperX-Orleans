using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Utilities;

namespace Experiment.Accelerator;

internal class DynamoManager
{
    readonly AmazonDynamoDBClient client;

    public DynamoManager(string AccessKey, string SecretKey)
    {
        client = new AmazonDynamoDBClient(AccessKey, SecretKey, Amazon.RegionEndpoint.EUNorth1);
    }

    public async Task CleanUp()
    {
        try
        {
            await client.DeleteTableAsync(Constants.SiloMembershipTable);
            Console.WriteLine($"DynamoManager: The {Constants.SiloMembershipTable} is deleted");
        }
        catch (ResourceNotFoundException)
        {
            Console.WriteLine($"DynamoManager: The {Constants.SiloMembershipTable} does not exist, no need to delete");
        }
    }
}
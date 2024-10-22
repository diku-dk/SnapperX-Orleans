using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;

namespace Utilities;

public static class AWSClientManager
{
    public static async Task DeleteMembershipTable(SnapperRoleType role, string regionID, string accessKey, string secretKey)
    {
        var tableName = Helper.GetMembershipTableName(regionID);
        var dynamoDBClient = new AmazonDynamoDBClient(accessKey, secretKey, Helper.ParseRegionEndpoint(regionID));
        try
        {
            await dynamoDBClient.DeleteTableAsync(tableName);

            // wait until the table.Count = completely deleted
            var exception = false;
            while (!exception)
            {
                try
                {
                    await dynamoDBClient.DescribeTableAsync(tableName);
                }
                catch (ResourceNotFoundException)
                {
                    exception = true;
                }
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
            Console.WriteLine($"{role}: The {tableName} is deleted");
        }
        catch (ResourceNotFoundException)
        {
            Console.WriteLine($"{role}: The {tableName} does not exist, no need to delete");
        }
    }
}
using Amazon.MemoryDB;
using Amazon.MemoryDB.Model;
using System.Diagnostics;

namespace Experiment.Accelerator;

internal class RedisManager
{
    public string connectionString;
    readonly AmazonMemoryDBClient client;

    public RedisManager(string AccessKey, string SecretKey)
    {
        client = new AmazonMemoryDBClient(AccessKey, SecretKey, Amazon.RegionEndpoint.EUNorth1);
    }

    public async Task CleanUp()   // delete all data stored in redis cluster
    {
        await GetConnectionString();
        /*
        var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { connectionString }, AllowAdmin = true });
        var server = redis.GetServer(connectionString);
        await server.FlushAllDatabasesAsync();
        Console.WriteLine("Flush all data in redis cluster");
        */
    }

    public async Task SpawnNew(string securityGroupID)
    {
        await InitSubnetGroup();

        try
        {
            await client.DescribeClustersAsync(new DescribeClustersRequest { ClusterName = "snapper" });
        }
        catch (ClusterNotFoundException)
        {
            var req = new CreateClusterRequest
            {
                ClusterName = "snapper",
                SubnetGroupName = "snapper",
                NodeType = "db.t4g.small",
                NumShards = 1,
                NumReplicasPerShard = 0,
                SecurityGroupIds = new List<string> { securityGroupID },
                TLSEnabled = false,
                AutoMinorVersionUpgrade = true,
                ACLName = "open-access"
            };

            await client.CreateClusterAsync(req);
            Console.WriteLine($"RedisManager: The Redis Cluster is created");
        }
    }

    async Task GetConnectionString()
    {
        connectionString = "";
        while (connectionString.Length == 0)
        {
            var clusters = (await client.DescribeClustersAsync(new DescribeClustersRequest { ClusterName = "snapper" })).Clusters;
            Debug.Assert(clusters.Count == 1);
            var endPoint = clusters.First().ClusterEndpoint;
            if (endPoint != null) connectionString = endPoint.Address + ":" + endPoint.Port;
        }
    }

    async Task InitSubnetGroup()
    {
        try
        {
            var groups = (await client.DescribeSubnetGroupsAsync(new DescribeSubnetGroupsRequest { SubnetGroupName = "snapper" })).SubnetGroups;
            Debug.Assert(groups.Count == 1 && groups.First().Name == "snapper");
            Console.WriteLine($"RedisManager: The subnet group 'snapper' already exists");
        }
        catch (SubnetGroupNotFoundException)
        {
            Console.WriteLine($"RedisManager: Create a subnet group 'snapper'");
            await client.CreateSubnetGroupAsync(new CreateSubnetGroupRequest
            {
                SubnetGroupName = "snapper",
                SubnetIds = new List<string> { "subnet-5105a12a", "subnet-27f1376a" }   // ID of eu-north-1b and eu-north-1c
            }) ;
        }
    }
}

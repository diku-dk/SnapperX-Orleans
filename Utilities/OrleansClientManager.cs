using Orleans.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Utilities;

public static class OrleansClientManager
{
    public static async Task<IClusterClient> GetOrleansClient(string regionID, string AccessKey, string SecretKey, ImplementationType implementationType)
    {
        var maxAttempt = 5;
        var rnd = new Random();

        for (int i = 0; i < maxAttempt; i++)
        {
            try
            {
                var host = new HostBuilder().UseOrleansClient(builder =>
                {
                    builder.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = regionID;
                        options.ServiceId = regionID;
                    });

                    Action<DynamoDBGatewayOptions> dynamoDBOptions = options => {
                        options.AccessKey = AccessKey;
                        options.SecretKey = SecretKey;
                        options.TableName = Helper.GetMembershipTableName(regionID);
                        options.Service =  regionID;
                    };

                    builder.UseDynamoDBClustering(dynamoDBOptions);

                    if (implementationType == ImplementationType.ORLEANSTXN) builder.UseTransactions();

                }).Build();
                await host.StartAsync();
                Console.WriteLine($"Client successfully connect to silo host {regionID}");
                return host.Services.GetRequiredService<IClusterClient>();
            }
            catch (Exception)
            {
                Console.WriteLine($"Attempt {i} failed to initialize the Orleans client.");
                var time = new Random().Next(0, 10);
                Thread.Sleep(TimeSpan.FromSeconds(time));
            }
        }
        throw new Exception($"Fail to create OrleansClient");
    }
}

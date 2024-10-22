using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace SnapperSiloHost.OrleansStorage;

public static class SiloBuilderExtensions
{
    public static ISiloBuilder AddMemoryTransactionalStateStorageAsDefault(
        this ISiloBuilder builder,
        Action<MyTransactionalStateOptions> configureOptions)
    {
        return builder.AddMemoryTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
    }

    static ISiloBuilder AddMemoryTransactionalStateStorage(this ISiloBuilder builder, string name, Action<MyTransactionalStateOptions> configureOptions)
    {
        return builder.ConfigureServices(services => services.AddMemoryTransactionalStateStorage(name, ob => ob.Configure(configureOptions)));
    }

    static IServiceCollection AddMemoryTransactionalStateStorage(this IServiceCollection services, string name,
        Action<OptionsBuilder<MyTransactionalStateOptions>> configureOptions = null)
    {
        configureOptions?.Invoke(services.AddOptions<MyTransactionalStateOptions>(name));
        //services.AddTransient<IConfigurationValidator>(sp => new AzureTableTransactionalStateOptionsValidator(sp.GetRequiredService<IOptionsMonitor<MyTransactionalStateOptions>>().Get(name), name));

        services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetKeyedService<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
        services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (sp, key) => MemoryTransactionalStateStorageFactory.Create(sp, key as string));
        services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(s => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredKeyedService<ITransactionalStateStorageFactory>(name));

        return services;
    }

    public static ISiloBuilder AddFileTransactionalStateStorageAsDefault(this ISiloBuilder builder, Action<MyTransactionalStateOptions> configureOptions)
    {
        return builder.AddFileTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
    }

    static ISiloBuilder AddFileTransactionalStateStorage(this ISiloBuilder builder, string name, Action<MyTransactionalStateOptions> configureOptions)
    {
        return builder.ConfigureServices(services => services.AddFileTransactionalStateStorage(name, ob => ob.Configure(configureOptions)));
    }

    static IServiceCollection AddFileTransactionalStateStorage(
        this IServiceCollection services,
        string name,
        Action<OptionsBuilder<MyTransactionalStateOptions>> configureOptions = null)
    {
        configureOptions?.Invoke(services.AddOptions<MyTransactionalStateOptions>(name));
        //services.AddTransient<IConfigurationValidator>(sp => new AzureTableTransactionalStateOptionsValidator(sp.GetRequiredService<IOptionsMonitor<MyTransactionalStateOptions>>().Get(name), name));

        services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetKeyedService<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
        services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (sp, key) => FileTransactionalStateStorageFactory.Create(sp, key as string));
        services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(s => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredKeyedService<ITransactionalStateStorageFactory>(name));

        return services;
    }
}

public class MyTransactionalStateOptions
{
    public int InitStage { get; set; }
    public int regionID { get; set; }
    public int siloID { get; set; }

    public int numPartitionPerSilo { get; set; }
}
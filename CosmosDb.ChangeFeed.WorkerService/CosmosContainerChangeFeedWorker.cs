using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;

namespace CosmosDb.ChangeFeed.WorkerService;

public class CosmosContainerChangeFeedWorker(ILogger<CosmosContainerChangeFeedWorker> logger, IConfiguration configuration) : BackgroundService
{
    private const string DatabaseName = "ToDoItems";
    private const string SourceContainerName = "ToDos";
    private const string LeasesContainerName = "ToDoLeases";

    private readonly CosmosClient _cosmosClient = new CosmosClientBuilder(configuration["CosmosDb:ConnectionString"])
        .Build();

    /*Cosmos Client that utilizes managed identity. Cosmos uses a separate RBAC than regular Azure. To utilize managed identity, custom roles must be set up.
         https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/security/how-to-grant-data-plane-role-based-access?tabs=custom-definition%2Ccsharp&pivots=azure-interface-cli
         */
    // _cosmosClient = new CosmosClientBuilder(_configuration["CosmosDb:EndpointUri"], new DefaultAzureCredential())
    //.Build();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await InitializeContainersAsync();

        while (!stoppingToken.IsCancellationRequested)
        {
            var processor = await StartChangeFeedProcessorAsync();
        
            _ = GenerateItemsAsync(processor);
        }
    }

    private async Task<ChangeFeedProcessor> StartChangeFeedProcessorAsync()
    {
        var leaseContainer = _cosmosClient.GetContainer(DatabaseName, LeasesContainerName);
        var changeFeedProcessor = _cosmosClient.GetContainer(DatabaseName, SourceContainerName)
            .GetChangeFeedProcessorBuilder<ToDoItem>(processorName: "changeFeedSample", onChangesDelegate: HandleChangesAsync)
            .WithInstanceName("Console")
            .WithLeaseContainer(leaseContainer)
            .Build();

       logger.LogInformation("Starting Change Feed Processor...");
        await changeFeedProcessor.StartAsync();
        return await Task.FromResult(changeFeedProcessor);
    }
    
    private async Task GenerateItemsAsync(ChangeFeedProcessor changeFeedProcessor)
    {
        var sourceContainer = _cosmosClient.GetContainer(DatabaseName,  SourceContainerName);
        while(true)
        {
            Console.WriteLine("Enter a number of items to insert in the container or 'exit' to stop:");
            var command = Console.ReadLine();
            if ("exit".Equals(command, StringComparison.InvariantCultureIgnoreCase))
            {
                Console.WriteLine();
                break;
            }

            if (int.TryParse(command, out int itemsToInsert))
            {
                Console.WriteLine($"Generating {itemsToInsert} items...");
                for (int i = 0; i < itemsToInsert; i++)
                {
                    var id = Guid.NewGuid().ToString();
                    await sourceContainer.CreateItemAsync(
                        new ToDoItem()
                        {
                            id = id,
                            CreationTime = DateTime.UtcNow
                        },
                        new PartitionKey(id));
                }
            }
        }

        logger.LogInformation("Stopping Change Feed Processor...");
        await changeFeedProcessor.StopAsync();
       logger.LogInformation("Stopped Change Feed Processor.");
    }



    private async Task InitializeContainersAsync()
    {
        var response = await _cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseName);

        await response.Database.CreateContainerIfNotExistsAsync(new ContainerProperties(SourceContainerName, "/id"));

        await response.Database.CreateContainerIfNotExistsAsync(new ContainerProperties(LeasesContainerName, "/id"));
    }

    private async Task HandleChangesAsync(ChangeFeedProcessorContext context, IReadOnlyCollection<ToDoItem> changes, CancellationToken cancellationToken)
    {
        logger.LogInformation("Started handling changes for lease {Lease}...", context.LeaseToken);

        foreach (var item in changes)
        {
            logger.LogInformation("Processing item {Id} created at {Time}", item.id, item.CreationTime);
            await Task.Delay(1, cancellationToken);
        }
    }
}
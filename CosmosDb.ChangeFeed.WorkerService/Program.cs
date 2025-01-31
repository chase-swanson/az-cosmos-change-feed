using CosmosDb.ChangeFeed.WorkerService;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.Seq("http://localhost:5341")
    .Enrich.FromLogContext()
    .CreateLogger();
try
{
    var builder = Host.CreateApplicationBuilder(args);
   
    builder.Services.AddHostedService<CosmosContainerChangeFeedWorker>();

    builder.Services.AddSerilog();

    Log.Information("Starting Cosmos Container Change Feed Worker Service");

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception e)
{
    Console.WriteLine(e);
    throw;
}

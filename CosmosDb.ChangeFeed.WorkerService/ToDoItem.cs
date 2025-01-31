namespace CosmosDb.ChangeFeed.WorkerService;

public class ToDoItem
{
    public string? id { get; set; }
    public DateTime CreationTime { get; set; }
    public string? Status { get; set; }
}
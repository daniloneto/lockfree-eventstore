using LockFree.EventStore;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var store = new EventStore<MetricEvent>(new EventStoreOptions<MetricEvent>
{
    TimestampSelector = new MetricTimestampSelector(),
    CapacityPerPartition = 1000,
    Partitions = Environment.ProcessorCount
});

app.MapPost("/metrics", (MetricEvent m) =>
{
    store.TryAppend(m);
    return Results.Accepted();
});

app.MapGet("/metrics/sum", (DateTime? from, DateTime? to, string label) =>
{
    double sum = store.Aggregate(() => 0d, (acc, e) =>
    {
        if (e.Label == label) return acc + e.Value; else return acc;
    }, from: from, to: to);
    return Results.Ok(sum);
});

app.MapGet("/metrics/top", (DateTime? from, DateTime? to, int k) =>
{
    var dict = store.AggregateBy(e => e.Label, () => 0d, (acc, e) => acc + e.Value, from: from, to: to);
    var top = dict.OrderByDescending(kv => kv.Value).Take(k);
    return Results.Ok(top);
});

app.Run();

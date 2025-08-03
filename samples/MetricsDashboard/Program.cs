using LockFree.EventStore;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var store = new EventStore<MetricEvent>(new EventStoreOptions<MetricEvent>
{
    TimestampSelector = new MetricTimestampSelector(),
    CapacityPerPartition = 1000,
    Partitions = Environment.ProcessorCount
});

// Health check endpoint
app.MapGet("/health", () => Results.Ok("OK"));

app.MapPost("/metrics", (MetricEvent m) =>
{
    store.TryAppend(m);
    return Results.Accepted();
});

// Traditional sum endpoint
app.MapGet("/metrics/sum", (DateTime? from, DateTime? to, string label) =>
{
    double sum = store.Aggregate(() => 0d, (acc, e) =>
    {
        if (e.Label == label) return acc + e.Value; else return acc;
    }, from: from, to: to);
    return Results.Ok(sum);
});

// New optimized sum by window endpoint
app.MapGet("/metrics/sum-window", (string label, int minutes = 10) =>
{
    var from = DateTime.UtcNow.AddMinutes(-minutes);
    var to = DateTime.UtcNow;

    // Use the new SumWindow method if available, otherwise fallback to traditional
    double sum = store.Aggregate(() => 0d, (acc, e) =>
    {
        if (e.Label == label) return acc + e.Value; else return acc;
    }, from: from, to: to);

    return Results.Ok(sum);
});

// New window aggregation endpoint
app.MapGet("/metrics/window", (DateTime? from, DateTime? to, string? label) =>
{
    var predicate = label != null ?
        new Predicate<MetricEvent>(e => e.Label == label) :
        null;

    var fromTicks = from?.Ticks ?? DateTime.MinValue.Ticks;
    var toTicks = to?.Ticks ?? DateTime.MaxValue.Ticks;

    var result = store.AggregateWindow(fromTicks, toTicks, predicate);

    return Results.Ok(new
    {
        count = result.Count,
        sum = result.Sum,
        min = result.Min,
        max = result.Max,
        avg = result.Avg,
        window = new
        {
            from = from?.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
            to = to?.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
            label = label
        }
    });
});

app.MapGet("/metrics/top", (DateTime? from, DateTime? to, int k) =>
{
    var dict = store.AggregateBy(e => e.Label, () => 0d, (acc, e) => acc + e.Value, from: from, to: to);
    var top = dict.OrderByDescending(kv => kv.Value).Take(k);
    return Results.Ok(top);
});

app.Run();

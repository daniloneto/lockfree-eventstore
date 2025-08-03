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

// Endpoint for snapshot views (zero-allocation)
app.MapGet("/metrics/snapshot-views", () =>
{
    var views = store.SnapshotViews();
    var totalEvents = views.Sum(v => v.Count);
    
    return Results.Ok(new
    {
        partitions = views.Count,
        totalEvents = totalEvents,
        views = views.Select(v => new
        {
            segmentCount = v.HasWrapAround ? 2 : 1,
            totalCount = v.Count,
            hasWrapAround = v.HasWrapAround,
            isEmpty = v.IsEmpty
        }).ToArray()
    });
});

// Endpoint for zero-allocation aggregation
app.MapGet("/metrics/aggregate-zero-alloc", (DateTime? from, DateTime? to) =>
{
    var fromTicks = from?.Ticks ?? DateTime.MinValue.Ticks;
    var toTicks = to?.Ticks ?? DateTime.MaxValue.Ticks;
    
    var result = (Sum: 0.0, Min: double.MaxValue, Max: double.MinValue, Count: 0);
    
    // Use zero-allocation processing
    result = store.ProcessEvents(
        result,
        (ref (double Sum, double Min, double Max, int Count) state, MetricEvent evt, DateTime? timestamp) =>
        {
            var eventTicks = timestamp?.Ticks ?? evt.Timestamp.Ticks;
            if (eventTicks >= fromTicks && eventTicks <= toTicks)
            {
                state.Sum += evt.Value;
                state.Count++;
                if (evt.Value < state.Min) state.Min = evt.Value;
                if (evt.Value > state.Max) state.Max = evt.Value;
            }
            return true; // Continue processing
        },
        null, // No filter
        from,
        to
    );
    
    return Results.Ok(new
    {
        count = result.Count,
        sum = result.Sum,
        min = result.Count > 0 ? result.Min : 0,
        max = result.Count > 0 ? result.Max : 0,
        avg = result.Count > 0 ? result.Sum / result.Count : 0
    });
});

// Endpoint for event processing with callbacks
app.MapGet("/metrics/process-events", (string? label) =>
{
    var result = (Processed: 0, HighCpuCount: 0);
    
    result = store.ProcessEvents(
        result,
        (ref (int Processed, int HighCpuCount) state, MetricEvent evt, DateTime? timestamp) =>
        {
            state.Processed++;
            if (label == null || evt.Label.Contains(label))
            {
                if (evt.Value > 50) // High CPU threshold
                {
                    state.HighCpuCount++;
                }
            }
            return true; // Continue processing
        }
    );
    
    return Results.Ok(new
    {
        processed = result.Processed,
        highCpuCount = result.HighCpuCount
    });
});

app.Run();

namespace MetricsDashboard;

using System.Diagnostics;
using System.Linq;
using LockFree.EventStore;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        var store = new EventStore<MetricEvent>(new EventStoreOptions<MetricEvent>
        {
            TimestampSelector = new MetricTimestampSelector(),
            CapacityPerPartition = 1000,
            Partitions = Environment.ProcessorCount
        });

        // New: order events store used by GatewayClient sample (/streams/{stream})
        var orderStore = new EventStore<OrderEvent>(new EventStoreOptions<OrderEvent>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 10_000,
            Partitions = Environment.ProcessorCount
        });

        var app = builder.Build();

        MapHealth(app);
        MapStreamRoutes(app, orderStore);
        MapMetricsRoutes(app, store);
        MapAdminRoutes(app, store, orderStore);

        await app.RunAsync();
    }

    private static void MapHealth(WebApplication app)
    {
        app.MapGet("/health", () => Results.Ok("OK"));
    }

    // --- Generic stream ingestion endpoints for Gateways ---
    private static void MapStreamRoutes(WebApplication app, EventStore<OrderEvent> orderStore)
    {
        // Append single event to a named stream
        app.MapPost("/streams/{*stream}", (string stream, GatewayOrderCreated dto) =>
        {
            var evt = new OrderEvent(dto.Id, stream, dto.GatewayId, dto.Valor, dto.Timestamp == default ? DateTime.UtcNow : dto.Timestamp.ToUniversalTime());
            orderStore.TryAppend(evt);
            return Results.Accepted();
        });

        // Read all events of a stream (simple demo, no paging yet)
        // Aggregate per stream (optional helper used for consistency checks)
        // Re-map: /streams/{*stream}?aggregate=true
        app.MapGet("/streams/{*stream}", (string stream, long? from, bool? aggregate) =>
        {
            if (aggregate == true)
            {
                var start = Stopwatch.GetTimestamp();
                var agg = new Dictionary<string, (int Count, long Sum)>();
                orderStore.ProcessEvents(agg, (ref Dictionary<string, (int Count, long Sum)> state, OrderEvent e, DateTime? _) =>
                {
                    if (e.Stream == stream)
                    {
                        if (!state.TryGetValue(e.GatewayId, out var val)) val = (0, 0);
                        val.Count++;
                        val.Sum += e.Valor;
                        state[e.GatewayId] = val;
                    }
                    return true;
                });
                var elapsedMs = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                var totalCount = agg.Values.Sum(v => v.Count);
                var totalValor = agg.Values.Sum(v => v.Sum);
                var byGateway = agg.ToDictionary(kv => kv.Key, kv => new { kv.Value.Count, kv.Value.Sum });
                return Results.Ok(new { stream, totalEvents = totalCount, totalValor, gateways = byGateway, durationMs = elapsedMs });
            }

            // from ignored; return raw list
            var list = new List<GatewayOrderCreated>();
            orderStore.ProcessEvents(list, (ref List<GatewayOrderCreated> acc, OrderEvent e, DateTime? ts) =>
            {
                if (e.Stream == stream)
                {
                    acc.Add(new GatewayOrderCreated
                    {
                        Id = e.Id,
                        GatewayId = e.GatewayId,
                        Valor = e.Valor,
                        Timestamp = e.Timestamp
                    });
                }
                return true;
            });
            return Results.Ok(list);
        });
    }

    private static void MapMetricsRoutes(WebApplication app, EventStore<MetricEvent> store)
    {
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
                    // Merge nested conditions to satisfy S1066
                    if ((label == null || evt.Label.Contains(label)) && evt.Value > 50)
                    {
                        state.HighCpuCount++;
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
    }

    // Administrative endpoints (simple, unsecured – secure before production)
    private static void MapAdminRoutes(WebApplication app, EventStore<MetricEvent> store, EventStore<OrderEvent> orderStore)
    {
        app.MapPost("/admin/clear", () => { store.Clear(); orderStore.Clear(); return Results.Ok(new { cleared = true }); });
        app.MapPost("/admin/reset", () => { store.Reset(); orderStore.Reset(); return Results.Ok(new { reset = true }); });
        app.MapPost("/admin/purge", (int? olderThanMinutes, DateTime? olderThan) =>
        {
            var cutoff = olderThan ?? DateTime.UtcNow.AddMinutes(-(olderThanMinutes ?? 60));
            try
            {
                store.Purge(cutoff);
                orderStore.Purge(cutoff);
                return Results.Ok(new { purgedBefore = cutoff });
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });
    }
}

// DTOs and helpers in the same namespace
internal class GatewayOrderCreated
{
    public string Id { get; set; } = string.Empty;
    public int Valor { get; set; }
    public DateTime Timestamp { get; set; }
    public string GatewayId { get; set; } = string.Empty;
}

internal readonly record struct OrderEvent(string Id, string Stream, string GatewayId, int Valor, DateTime Timestamp);

internal readonly struct OrderTimestampSelector : IEventTimestampSelector<OrderEvent>
{
    public DateTime GetTimestamp(OrderEvent e) => e.Timestamp;
    public long GetTimestampTicks(OrderEvent e) => e.Timestamp.Ticks;
}

using System.Diagnostics;
using System.Globalization;
using LockFree.EventStore;

namespace MetricsDashboard;

internal static class Program
{
    /// <summary>
    /// Application entry point: builds and runs the MetricsDashboard web application.
    /// </summary>
    /// <remarks>
    /// Configures in-memory event stores for metrics and gateway/order events, registers HTTP route groups
    /// (health, stream/order endpoints, metric endpoints, and admin endpoints), then starts the web host
    /// and runs until shutdown.
    /// </remarks>
    /// <param name="args">Command-line arguments forwarded to the web host builder.</param>
    /// <returns>A task that completes when the web application shuts down.</returns>
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

    /// <summary>
    /// Registers a GET health-check endpoint at "/health" on the provided <see cref="WebApplication"/>.
    /// The endpoint returns HTTP 200 with the plain text body "OK".
    /// </summary>
    private static void MapHealth(WebApplication app)
    {
        _ = app.MapGet("/health", () => Results.Ok("OK"));
    }

    // --- Generic stream ingestion endpoints for Gateways ---
    private static void MapStreamRoutes(WebApplication app, EventStore<OrderEvent> orderStore)
    {
        // Append single event to a named stream
        _ = app.MapPost("/streams/{*stream}", (string stream, GatewayOrderCreated dto) =>
        {
            var evt = new OrderEvent(dto.Id, stream, dto.GatewayId, dto.Valor, dto.Timestamp == default ? DateTime.UtcNow : dto.Timestamp.ToUniversalTime());
            _ = orderStore.TryAppend(evt);
            return Results.Accepted();
        });

        // Read all events of a stream (simple demo, no paging yet)
        // Aggregate per stream (optional helper used for consistency checks)
        // Re-map: /streams/{*stream}?aggregate=true
        _ = app.MapGet("/streams/{*stream}", (string stream, long? from, bool? aggregate) =>
        {
            if (aggregate == true)
            {
                var start = Stopwatch.GetTimestamp();
                var agg = new Dictionary<string, (int Count, long Sum)>();
                agg = orderStore.ProcessEvents(agg, (ref state, e, _) =>
                {
                    // Guard clause to reduce nesting
                    if (e.Stream != stream)
                    {
                        return true;
                    }

                    if (!state.TryGetValue(e.GatewayId, out var val))
                    {
                        val = (0, 0);
                    }
                    val.Count++;
                    val.Sum += e.Valor;
                    state[e.GatewayId] = val;
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
            list = orderStore.ProcessEvents(list, (ref acc, e, ts) =>
            {
                // Guard clause to reduce nesting
                if (e.Stream != stream)
                {
                    return true;
                }

                acc.Add(new GatewayOrderCreated
                {
                    Id = e.Id,
                    GatewayId = e.GatewayId,
                    Valor = e.Valor,
                    Timestamp = e.Timestamp
                });
                return true;
            });
            return Results.Ok(list);
        });
    }

    private static void MapMetricsRoutes(WebApplication app, EventStore<MetricEvent> store)
    {
        _ = app.MapPost("/metrics", (MetricEvent m) => PostMetrics(store, m));

        _ = app.MapGet("/metrics/sum", (DateTime? from, DateTime? to, string label) =>
            GetMetricsSum(store, from, to, label));

        _ = app.MapGet("/metrics/sum-window", (string label, int minutes = 10) =>
            GetMetricsSumWindow(store, label, minutes));

        _ = app.MapGet("/metrics/window", (DateTime? from, DateTime? to, string? label) =>
            GetMetricsWindow(store, from, to, label));

        _ = app.MapGet("/metrics/snapshot-views", () => GetMetricsSnapshotViews(store));

        _ = app.MapGet("/metrics/aggregate-zero-alloc", (DateTime? from, DateTime? to) =>
            GetMetricsAggregateZeroAlloc(store, from, to));

        _ = app.MapGet("/metrics/process-events", (string? label) =>
            GetMetricsProcessEvents(store, label));
    }

    private static IResult PostMetrics(EventStore<MetricEvent> store, MetricEvent m)
    {
        _ = store.TryAppend(m);
        return Results.Accepted();
    }

    private static IResult GetMetricsSum(EventStore<MetricEvent> store, DateTime? from, DateTime? to, string label)
    {
        // Prefer zero-alloc ProcessEvents to compute sum
        var sum = store.ProcessEvents(0.0, (ref s, e, _) =>
        {
            if (e.Label == label)
            {
                s += e.Value;
            }
            return true;
        }, null, from, to);
        return Results.Ok(sum);
    }

    private static IResult GetMetricsSumWindow(EventStore<MetricEvent> store, string label, int minutes)
    {
        var from = DateTime.UtcNow.AddMinutes(-minutes);
        var to = DateTime.UtcNow;
        var sum = store.ProcessEvents(0.0, (ref s, e, _) =>
        {
            if (e.Label == label)
            {
                s += e.Value;
            }
            return true;
        }, null, from, to);
        return Results.Ok(sum);
    }

    private static IResult GetMetricsWindow(EventStore<MetricEvent> store, DateTime? from, DateTime? to, string? label)
    {
        var filter = label is null ? null : new EventFilter<MetricEvent>((e, ts) => e.Label == label);
        var result = store.AggregateWindowZeroAlloc(m => m.Value, filter, from, to);
        return Results.Ok(new
        {
            count = result.Count,
            sum = result.Sum,
            min = result.Min,
            max = result.Max,
            avg = result.Avg,
            window = new
            {
                from = from?.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture),
                to = to?.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture),
                label
            }
        });
    }

    private static IResult GetMetricsSnapshotViews(EventStore<MetricEvent> store)
    {
        var views = store.SnapshotViews();
        var totalEvents = views.Sum(v => v.Count);
        return Results.Ok(new
        {
            partitions = views.Count,
            totalEvents,
            views = views.Select(v => new
            {
                segmentCount = v.HasWrapAround ? 2 : 1,
                totalCount = v.Count,
                hasWrapAround = v.HasWrapAround,
                isEmpty = v.IsEmpty
            }).ToArray()
        });
    }

    private static IResult GetMetricsAggregateZeroAlloc(EventStore<MetricEvent> store, DateTime? from, DateTime? to)
    {
        var fromTicks = from?.Ticks ?? DateTime.MinValue.Ticks;
        var toTicks = to?.Ticks ?? DateTime.MaxValue.Ticks;
        var result = (Sum: 0.0, Min: double.MaxValue, Max: double.MinValue, Count: 0);
        result = store.ProcessEvents(
            result,
            (ref state, evt, timestamp) =>
            {
                var eventTicks = timestamp?.Ticks ?? evt.Timestamp.Ticks;
                if (eventTicks >= fromTicks && eventTicks <= toTicks)
                {
                    state.Sum += evt.Value;
                    state.Count++;
                    if (evt.Value < state.Min)
                    {
                        state.Min = evt.Value;
                    }
                    if (evt.Value > state.Max)
                    {
                        state.Max = evt.Value;
                    }
                }
                return true;
            },
            null,
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
    }

    private static IResult GetMetricsProcessEvents(EventStore<MetricEvent> store, string? label)
    {
        var result = (Processed: 0, HighCpuCount: 0);
        result = store.ProcessEvents(
            result,
            (ref state, evt, timestamp) =>
            {
                state.Processed++;
                if ((label == null || evt.Label.Contains(label)) && evt.Value > 50)
                {
                    state.HighCpuCount++;
                }
                return true;
            }
        );
        return Results.Ok(new { processed = result.Processed, highCpuCount = result.HighCpuCount });
    }

    // Administrative endpoints (simple, unsecured – secure before production)
    private static void MapAdminRoutes(WebApplication app, EventStore<MetricEvent> store, EventStore<OrderEvent> orderStore)
    {
        _ = app.MapPost("/admin/clear", () => { store.Clear(); orderStore.Clear(); return Results.Ok(new { cleared = true }); });
        _ = app.MapPost("/admin/reset", () => { store.Reset(); orderStore.Reset(); return Results.Ok(new { reset = true }); });
        _ = app.MapPost("/admin/purge", (int? olderThanMinutes, DateTime? olderThan) =>
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
internal sealed class GatewayOrderCreated
{
    public string Id { get; set; } = string.Empty;
    public int Valor { get; set; }
    public DateTime Timestamp { get; set; }
    public string GatewayId { get; set; } = string.Empty;
}

internal readonly record struct OrderEvent(string Id, string Stream, string GatewayId, int Valor, DateTime Timestamp);

internal readonly struct OrderTimestampSelector : IEventTimestampSelector<OrderEvent>
{
    public DateTime GetTimestamp(OrderEvent e)
    {
        return e.Timestamp;
    }

    public long GetTimestampTicks(OrderEvent e)
    {
        return e.Timestamp.Ticks;
    }
}

using LockFree.EventStore;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Each gateway client has its own logical instance id
var gatewayId = Environment.GetEnvironmentVariable("GATEWAY_ID") ?? Guid.NewGuid().ToString("n");
var eventStoreUrl = Environment.GetEnvironmentVariable("EVENTSTORE_URL") ?? "http://eventstore:7070"; // docker service name

// Local in-memory aggregation of what this gateway sent (for quick per-gateway stats)
long sentCount = 0;
object sync = new();

// Shared HttpClient for posting to central event store server (MetricsDashboard or future dedicated API)
var http = new HttpClient { BaseAddress = new Uri(eventStoreUrl) };
var stream = "gateway/orders";

// Health
app.MapGet("/health", () => Results.Ok(new { gatewayId, status = "OK" }));

// Endpoint to generate and send an event to central event store
// Simulates an order created event with random value
app.MapPost("/orders", async () =>
{
    var evt = new GatewayOrderCreated
    {
        Id = $"{gatewayId}-{Ulid.NewUlid()}",
        Valor = Random.Shared.Next(1, 5000),
        Timestamp = DateTime.UtcNow,
        GatewayId = gatewayId
    };
    // POST to event store stream endpoint (placeholder: /streams/{stream})
    var resp = await http.PostAsJsonAsync($"/streams/{stream}", evt);
    if (!resp.IsSuccessStatusCode)
    {
        return Results.Problem($"Failed to append event: {(int)resp.StatusCode}");
    }
    lock (sync) sentCount++;
    return Results.Accepted(value: evt);
});

// Endpoint to bulk-generate N events
app.MapPost("/orders/bulk", async (int n) =>
{
    int ok = 0;
    var tasks = Enumerable.Range(0, n).Select(async _ =>
    {
        var evt = new GatewayOrderCreated
        {
            Id = $"{gatewayId}-{Ulid.NewUlid()}",
            Valor = Random.Shared.Next(1, 5000),
            Timestamp = DateTime.UtcNow,
            GatewayId = gatewayId
        };
        var r = await http.PostAsJsonAsync($"/streams/{stream}", evt);
        if (r.IsSuccessStatusCode)
        {
            Interlocked.Increment(ref ok);
        }
    });
    await Task.WhenAll(tasks);
    lock (sync) sentCount += ok;
    return Results.Ok(new { requested = n, succeeded = ok });
});

// Endpoint to show local stats (per gateway)
app.MapGet("/stats/local", () => Results.Ok(new { gatewayId, sent = sentCount }));

// Use aggregate endpoint first, fallback to full read if not available
app.MapGet("/stats/global", async () =>
{
    try
    {
        var aggResp = await http.GetAsync($"/streams/{stream}?aggregate=true");
        if (aggResp.IsSuccessStatusCode)
        {
            var aggJson = await aggResp.Content.ReadAsStringAsync();
            return Results.Text(aggJson, "application/json");
        }
        var resp = await http.GetAsync($"/streams/{stream}?from=0");
        if (!resp.IsSuccessStatusCode)
            return Results.Problem($"Central read failed: {(int)resp.StatusCode}");
        var events = await resp.Content.ReadFromJsonAsync<List<GatewayOrderCreated>>();
        if (events == null) return Results.Ok(new { totalEvents = 0 });
        var total = events.Count;
        var totalValor = events.Sum(e => e.Valor);
        var porGateway = events.GroupBy(e => e.GatewayId)
            .ToDictionary(g => g.Key, g => new { count = g.Count(), sum = g.Sum(x => x.Valor) });
        return Results.Ok(new { stream, totalEvents = total, totalValor, gateways = porGateway });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message);
    }
});

app.Run();

internal class GatewayOrderCreated
{
    public string Id { get; set; } = string.Empty;
    public int Valor { get; set; }
    public DateTime Timestamp { get; set; }
    public string GatewayId { get; set; } = string.Empty;
}

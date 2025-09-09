using LockFree.EventStore;
using LockFree.EventStore.Snapshots;
using SnapshotSensorsApi;
using System.Text.Json.Serialization;

// Build minimal API
var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

// Configuration
var partitions = Environment.ProcessorCount;
var capacityPerPartition = 100_000; // per partition
var snapshotRoot = Path.Combine(AppContext.BaseDirectory, "snapshots-api");
Directory.CreateDirectory(snapshotRoot);

var store = new EventStore<Event>(new EventStoreOptions<Event>
{
    CapacityPerPartition = capacityPerPartition,
    Partitions = partitions,
    EnableFalseSharingProtection = true,
    StatsUpdateInterval = 25_000
});

var snapshotter = store.ConfigureSnapshots(
    new SnapshotOptions
    {
        Enabled = true,
        Interval = TimeSpan.FromSeconds(10),
        MinEventsBetweenSnapshots = 50_000,
        MaxConcurrentSnapshotJobs = Math.Max(2, partitions / 4),
        SnapshotsToKeep = 3,
        MaxSaveAttempts = 5,
        BackoffBaseDelay = TimeSpan.FromMilliseconds(100),
        BackoffFactor = 2.0,
        StableCaptureMaxAttempts = 8,
        FinalSnapshotOnShutdown = true,
        FinalSnapshotTimeout = TimeSpan.FromSeconds(5)
    },
    serializer: new BinarySnapshotSerializer(compress: true),
    store: new FileSystemSnapshotStore(snapshotRoot, fsyncDirectory: false)
);

// Register services BEFORE restore so hosted snapshot loop can start later
builder.Services.AddSingleton(store);
builder.Services.AddSingleton(snapshotter);
builder.Services.AddHostedService<SnapshotHostedService>();

// Warm start: restore previous snapshot contents
var restored = await store.RestoreFromSnapshotsAsync();
Console.WriteLine($"[BOOT] Partições restauradas: {restored}");

// Aggregator that keeps running totals (rebuilt from restored data)
var aggregator = new Aggregator();
aggregator.RebuildFromEvents(store.EnumerateSnapshot());
builder.Services.AddSingleton(aggregator);

var app = builder.Build();

// POST /sensor => append two events (temperature key=1, humidity key=2) and update aggregates
app.MapPost("/sensor", async (HttpRequest req, EventStore<Event> store, Aggregator agg, ILoggerFactory lf) =>
{
    var logger = lf.CreateLogger("Sensor");
    SensorReadingDto? dto = null;
    try
    {
        dto = await req.ReadFromJsonAsync<SensorReadingDto>();
    }
    catch (Exception ex)
    {
        ParseLog.JsonParseWarning(logger, ex);
        return Results.BadRequest(new { error = "Invalid JSON", detail = ex.Message });
    }
    if (dto is null)
    {
        SensorLog.MissingBody(logger);
        return Results.BadRequest(new { error = "Body required" });
    }
    if (string.IsNullOrWhiteSpace(dto.DeviceId))
    {
        SensorLog.MissingDevice(logger);
        return Results.BadRequest(new { error = "DeviceId required" });
    }
    if (double.IsNaN(dto.Temperature) || double.IsNaN(dto.Humidity))
    {
        SensorLog.InvalidNumeric(logger, dto.Temperature, dto.Humidity);
        return Results.BadRequest(new { error = "Invalid numeric values" });
    }
    var timestamp = (dto.TimestampUtc ?? DateTime.UtcNow).ToUniversalTime();
    var ticks = timestamp.Ticks;
    var tempEvent = new Event(new KeyId(1), dto.Temperature, ticks);
    var humEvent = new Event(new KeyId(2), dto.Humidity, ticks);
    var partition = Math.Abs(dto.DeviceId.GetHashCode()) % store.Partitions;
    _ = store.TryAppend(new KeyId(1), tempEvent, partition);
    _ = store.TryAppend(new KeyId(2), humEvent, partition);
    agg.Add(dto.Temperature, dto.Humidity);
    SensorLog.Accepted(logger, dto.DeviceId);
    return Results.Accepted($"/sensor/{dto.DeviceId}");
});

// GET /state => consolidated aggregates (min/max/avg/count) + approx totals
app.MapGet("/state", (Aggregator agg, EventStore<Event> store) =>
{
    var (temperature, humidity) = agg.GetState();
    _ = store.TryGetStats(out var stats);
    return Results.Ok(new
    {
        temperature,
        humidity,
        totalApprox = store.CountApprox,
        stats.AppendCount,
        stats.DroppedCount
    });
});

// GET /metrics => internal store + snapshot metrics
app.MapGet("/metrics", (EventStore<Event> store) =>
{
    _ = store.TryGetStats(out var stats);
    _ = store.TryGetSnapshotMetrics(out var snap);
    return Results.Ok(new
    {
        stats.AppendCount,
        stats.DroppedCount,
        stats.SnapshotBytesExposed,
        snapshot = snap
    });
});

app.Run();

namespace SnapshotSensorsApi
{
    /// <summary>
    /// Represents a sensor reading with device identification and environmental measurements.
    /// </summary>
    public sealed record SensorReadingDto(
        [property: JsonPropertyName("deviceId")] string DeviceId,
        [property: JsonPropertyName("temperature")] double Temperature,
        [property: JsonPropertyName("humidity")] double Humidity,
        [property: JsonPropertyName("timestampUtc")] DateTime? TimestampUtc);
}

// ================= Aggregator =================
internal sealed class Aggregator
{
    private readonly Lock _lock = new();
    private Stats _temp = Stats.Create();
    private Stats _hum = Stats.Create();

    public void Add(double temperature, double humidity)
    {
        using (_lock.EnterScope())
        {
            _temp.Update(temperature);
            _hum.Update(humidity);
        }
    }

    public void RebuildFromEvents(IEnumerable<Event> events)
    {
        using (_lock.EnterScope())
        {
            _temp = Stats.Create();
            _hum = Stats.Create();
            foreach (var e in events)
            {
                if (e.Key.Value == 1)
                {
                    _temp.Update(e.Value);
                }
                else if (e.Key.Value == 2)
                {
                    _hum.Update(e.Value);
                }
            }
        }
    }

    public (object temperature, object humidity) GetState()
    {
        using (_lock.EnterScope())
        {
            return (ToDto(_temp), ToDto(_hum));
        }
    }

    private static object ToDto(Stats s)
    {
        return new
        {
            count = s.Count,
            min = s.Count == 0 ? 0 : s.Min,
            max = s.Count == 0 ? 0 : s.Max,
            avg = s.Count == 0 ? 0 : s.Sum / s.Count
        };
    }

    private struct Stats
    {
        public long Count;
        public double Sum;
        public double Min;
        public double Max;

        public static Stats Create()
        {
            return new Stats { Min = double.MaxValue, Max = double.MinValue };
        }

        public void Update(double v)
        {
            if (Count == 0)
            {
                Min = Max = v;
            }
            else
            {
                if (v < Min)
                {
                    Min = v;
                }
                if (v > Max)
                {
                    Max = v;
                }
            }
            Count++;
            Sum += v;
        }
    }
}

// ================= ParseLog =================
internal static class ParseLog
{
    private static readonly Action<ILogger, string, Exception?> _jsonParseWarn = LoggerMessage.Define<string>(LogLevel.Warning, new EventId(101, "JsonParse"), "JSON parse failed: {Message}");
    public static void JsonParseWarning(ILogger logger, Exception ex)
    {
        _jsonParseWarn(logger, ex.Message, ex);
    }
}

// ================= SensorLog =================
internal static class SensorLog
{
    private static readonly Action<ILogger, string, Exception?> _accepted = LoggerMessage.Define<string>(LogLevel.Debug, new EventId(201, "Accepted"), "Accepted reading for device {DeviceId}");
    private static readonly Action<ILogger, Exception?> _missingBody = LoggerMessage.Define(LogLevel.Warning, new EventId(202, "MissingBody"), "Missing request body");
    private static readonly Action<ILogger, Exception?> _missingDevice = LoggerMessage.Define(LogLevel.Warning, new EventId(203, "MissingDevice"), "Missing device id");
    private static readonly Action<ILogger, double, double, Exception?> _invalidNumeric = LoggerMessage.Define<double, double>(LogLevel.Warning, new EventId(204, "InvalidNumeric"), "Invalid numeric values temp={Temp} hum={Hum}");
    public static void Accepted(ILogger l, string device)
    {
        _accepted(l, device, null);
    }

    public static void MissingBody(ILogger l)
    {
        _missingBody(l, null);
    }

    public static void MissingDevice(ILogger l)
    {
        _missingDevice(l, null);
    }

    public static void InvalidNumeric(ILogger l, double t, double h)
    {
        _invalidNumeric(l, t, h, null);
    }
}

using System.Diagnostics;
using LockFree.EventStore;
using LockFree.EventStore.Snapshots;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

// Domain record representing a sensor logical reading aggregated into Event's (KeyId, double, ticks)
// For snapshotting we will use the built-in Event struct directly to minimize allocations.

// This sample demonstrates:
// 1. Warm start: restore ring buffers from latest snapshots on process boot.
// 2. Periodic background snapshots without blocking high-frequency appends.
// 3. Graceful shutdown with a final snapshot attempt.
// 4. Reading snapshotter metrics.

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

// Configuration (could come from appsettings / env)
var partitions = Environment.ProcessorCount;          // one partition per CPU core
var capacityPerPartition = 50_000;                     // keep last 50k readings per partition
var snapshotRoot = Path.Combine(AppContext.BaseDirectory, "snapshots");
Directory.CreateDirectory(snapshotRoot);

// Create store specialized for Event (required for snapshot subsystem)
var store = new EventStore<Event>(new EventStoreOptions<Event>
{
    CapacityPerPartition = capacityPerPartition,
    Partitions = partitions,
    EnableFalseSharingProtection = true,
    StatsUpdateInterval = 25_000
});

// Configure snapshot subsystem
var snapshotter = store.ConfigureSnapshots(
    new SnapshotOptions
    {
        Enabled = true,
        Interval = TimeSpan.FromSeconds(5),                  // time trigger
        MinEventsBetweenSnapshots = 100_000,                 // event count trigger (global)
        MaxConcurrentSnapshotJobs = Math.Max(2, partitions / 4),
        SnapshotsToKeep = 3,
        MaxSaveAttempts = 5,
        BackoffBaseDelay = TimeSpan.FromMilliseconds(50),
        BackoffFactor = 2.0,
        StableCaptureMaxAttempts = 8,
        FinalSnapshotOnShutdown = true,
        FinalSnapshotTimeout = TimeSpan.FromSeconds(3)
    },
    serializer: new BinarySnapshotSerializer(compress: true),
    store: new FileSystemSnapshotStore(snapshotRoot, fsyncDirectory: false)
);

// Hosted background service wrapper
builder.Services.AddSingleton(snapshotter);
builder.Services.AddHostedService<SnapshotHostedService>();

// Add a hosted producer service simulating high-frequency sensor readings
builder.Services.AddHostedService(sp => new SensorProducer(store, sp.GetRequiredService<ILogger<SensorProducer>>()));
// Add a hosted metrics logger service
builder.Services.AddHostedService(sp => new MetricsPrinter(store, sp.GetRequiredService<ILogger<MetricsPrinter>>()));

var app = builder.Build();

// Warm start: restore previous state before running producers
var restored = await store.RestoreFromSnapshotsAsync();
Console.WriteLine($"[BOOT] Partitions restauradas de snapshot: {restored}");

await app.RunAsync();

internal sealed class SensorProducer(EventStore<Event> store, ILogger<SensorProducer> logger) : BackgroundService
{
    private readonly Random _rnd = new();
    private readonly KeyId _tempKey = new(1);
    private readonly KeyId _humKey = new(2);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _ = Task.Run(() => ProduceLoop(stoppingToken), stoppingToken);
        return Task.CompletedTask;
    }

    private void ProduceLoop(CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var count = 0L;
        while (!ct.IsCancellationRequested)
        {
            var nowTicks = DateTime.UtcNow.Ticks;
            // Simulate two readings (temperature & humidity) per iteration across many partitions
            var temp = 15.0 + (_rnd.NextDouble() * 10.0); // 15-25 C
            var hum = 40.0 + (_rnd.NextDouble() * 20.0);  // 40-60 %

            // Choose partition by hash to distribute evenly
            var partition = (_rnd.Next() & int.MaxValue) % store.Partitions;
            _ = store.TryAppend(_tempKey, new Event(_tempKey, temp, nowTicks), partition);
            _ = store.TryAppend(_humKey, new Event(_humKey, hum, nowTicks), partition);
            count += 2;

            if ((count & 0xFFFFF) == 0) // every ~1,048,576 events
            {
                Log.Produced(logger, count, sw.Elapsed.TotalSeconds.ToString("n1", System.Globalization.CultureInfo.InvariantCulture), null);
            }
        }
    }
}

internal sealed class MetricsPrinter(EventStore<Event> store, ILogger<MetricsPrinter> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            if (store.TryGetStats(out var stats))
            {
                Log.Stats(logger, stats.AppendCount, stats.DroppedCount, stats.SnapshotBytesExposed, null);
            }
            if (store.TryGetSnapshotMetrics(out var snapMetrics))
            {
                Log.Snap(logger, snapMetrics.DroppedJobs, snapMetrics.TotalFailedJobs, snapMetrics.StableCaptureFailures, null);
            }
        }
    }
}

internal static class Log
{
    internal static readonly Action<ILogger, long, string, Exception?> Produced = LoggerMessage.Define<long, string>(LogLevel.Information, new EventId(1, "Produced"), "Produced {Count} readings in {Elapsed}s");
    internal static readonly Action<ILogger, long, long, long, Exception?> Stats = LoggerMessage.Define<long, long, long>(LogLevel.Information, new EventId(2, "Stats"), "Stats Append={Append} Dropped={Dropped} SnapshotBytes={Bytes}");
    internal static readonly Action<ILogger, long, long, long, Exception?> Snap = LoggerMessage.Define<long, long, long>(LogLevel.Information, new EventId(3, "Snapshot"), "Snapshotter DroppedJobs={Dropped} FailedJobs={Failed} StableFailures={Stable}");
}

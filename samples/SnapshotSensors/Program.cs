using System.Diagnostics;
using LockFree.EventStore;
using LockFree.EventStore.Snapshots;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using SnapshotSensors; // added namespace for moved types

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
builder.Services.AddHostedService<SensorProducer>();
// Add a hosted metrics logger service
builder.Services.AddHostedService<MetricsPrinter>();

var app = builder.Build();

// Warm start: restore previous state before running producers
var restored = await store.RestoreFromSnapshotsAsync();
Console.WriteLine($"[BOOT] Partitions restauradas de snapshot: {restored}");

await app.RunAsync();

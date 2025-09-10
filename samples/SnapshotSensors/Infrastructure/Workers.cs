using System.Diagnostics;
using LockFree.EventStore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SnapshotSensors;

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
            var temp = 15.0 + (_rnd.NextDouble() * 10.0);
            var hum = 40.0 + (_rnd.NextDouble() * 20.0);
            var partition = (_rnd.Next() & int.MaxValue) % store.Partitions;
            _ = store.TryAppend(_tempKey, new Event(_tempKey, temp, nowTicks), partition);
            _ = store.TryAppend(_humKey, new Event(_humKey, hum, nowTicks), partition);
            count += 2;
            if ((count & 0xFFFFF) == 0)
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

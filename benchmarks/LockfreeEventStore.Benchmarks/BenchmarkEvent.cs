using LockFree.EventStore;

namespace LockfreeEventStore.Benchmarks;

/// <summary>
/// Event structure used for benchmarking with string key, double value and timestamp.
/// </summary>
public readonly record struct BenchmarkEvent(string Key, double Value, long TimestampTicks)
{
    public DateTime Timestamp => new(TimestampTicks);
}

/// <summary>
/// Timestamp selector for BenchmarkEvent.
/// </summary>
public sealed class BenchmarkEventTimestampSelector : IEventTimestampSelector<BenchmarkEvent>
{
    public DateTime GetTimestamp(BenchmarkEvent e) => e.Timestamp;
}

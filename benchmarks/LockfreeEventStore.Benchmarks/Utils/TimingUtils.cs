using System.Diagnostics;
using HdrHistogram;

namespace LockfreeEventStore.Benchmarks.Utils;

/// <summary>
/// High-resolution timing utilities for measuring latency.
/// </summary>
public static class TimingUtils
{
    private static readonly double TimestampToNanos = 1_000_000_000.0 / Stopwatch.Frequency;
    
    public static long GetTimestamp() => Stopwatch.GetTimestamp();
    
    public static double ToNanoseconds(long timestamp) => timestamp * TimestampToNanos;
    
    public static double ToMicroseconds(long timestamp) => timestamp * TimestampToNanos / 1000.0;
    
    public static double ToMilliseconds(long timestamp) => timestamp * TimestampToNanos / 1_000_000.0;
}

/// <summary>
/// Latency recorder using HdrHistogram for accurate percentile measurements.
/// </summary>
public sealed class LatencyRecorder
{
    private readonly LongHistogram _histogram;
    
    public LatencyRecorder(long maxValueNanos = 60_000_000_000L) // 60 seconds max
    {
        _histogram = new LongHistogram(1, maxValueNanos, 3);
    }
    
    public void Record(long startTimestamp, long endTimestamp)
    {
        var latencyNanos = (long)TimingUtils.ToNanoseconds(endTimestamp - startTimestamp);
        _histogram.RecordValue(Math.Max(1, latencyNanos));
    }
    
    public void Record(long latencyNanos)
    {
        _histogram.RecordValue(Math.Max(1, latencyNanos));
    }
    
    public double GetMean() => _histogram.GetMean();
    public double GetMedian() => _histogram.GetValueAtPercentile(50.0);
    public double GetP95() => _histogram.GetValueAtPercentile(95.0);
    public double GetP99() => _histogram.GetValueAtPercentile(99.0);
    public double GetMax() => _histogram.GetMaxValue();
    public long GetCount() => _histogram.TotalCount;
    
    public void Reset() => _histogram.Reset();
}

/// <summary>
/// Synchronization utilities for coordinating benchmark threads.
/// </summary>
public static class SyncUtils
{
    public static void CoordinatedStart(int threadCount, Action action)
    {
        using var barrier = new Barrier(threadCount + 1);
        var tasks = new Task[threadCount];
        
        for (int i = 0; i < threadCount; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                barrier.SignalAndWait(); // Wait for all threads to be ready
                action();
            });
        }
        
        barrier.SignalAndWait(); // Release all threads
        Task.WaitAll(tasks);
    }
    
    public static void CoordinatedStart<T>(int threadCount, Func<int, T> threadAction, out T[] results)
    {
        using var barrier = new Barrier(threadCount + 1);
        var tasks = new Task<T>[threadCount];
        
        for (int i = 0; i < threadCount; i++)
        {
            int threadId = i;
            tasks[i] = Task.Run(() =>
            {
                barrier.SignalAndWait(); // Wait for all threads to be ready
                return threadAction(threadId);
            });
        }
        
        barrier.SignalAndWait(); // Release all threads
        Task.WaitAll(tasks);
        results = tasks.Select(t => t.Result).ToArray();
    }
}

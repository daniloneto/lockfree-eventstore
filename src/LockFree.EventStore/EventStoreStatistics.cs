using System.Threading;

namespace LockFree.EventStore;

/// <summary>
/// Statistics and metrics for an EventStore instance.
/// </summary>
public sealed class EventStoreStatistics
{
    private long _totalAppended;
    private long _totalDiscarded;
    private DateTime _lastAppendTime;

    /// <summary>
    /// Total number of events that have been appended since creation.
    /// </summary>
    public long TotalAppended => Volatile.Read(ref _totalAppended);

    /// <summary>
    /// Total number of events that have been discarded due to capacity limits.
    /// </summary>
    public long TotalDiscarded => Volatile.Read(ref _totalDiscarded);

    /// <summary>
    /// Timestamp of the last successful append operation.
    /// </summary>
    public DateTime LastAppendTime => _lastAppendTime;

    /// <summary>
    /// Approximate events per second based on recent activity.
    /// </summary>
    public double AppendsPerSecond
    {
        get
        {
            var elapsed = DateTime.UtcNow - _lastAppendTime;
            if (elapsed.TotalSeconds < 1) return _totalAppended;
            return Math.Max(0, _totalAppended / elapsed.TotalSeconds);
        }
    }

    internal void RecordAppend()
    {
        Interlocked.Increment(ref _totalAppended);
        _lastAppendTime = DateTime.UtcNow;
    }

    internal void RecordDiscard()
    {
        Interlocked.Increment(ref _totalDiscarded);
    }

    internal void Reset()
    {
        Volatile.Write(ref _totalAppended, 0);
        Volatile.Write(ref _totalDiscarded, 0);
        _lastAppendTime = default;
    }
}

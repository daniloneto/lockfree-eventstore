namespace LockFree.EventStore;

/// <summary>
/// Statistics and metrics for an EventStore instance.
/// </summary>
public sealed class EventStoreStatistics
{
    private long _totalAppended;
    private long _totalDiscarded;

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
    public DateTime LastAppendTime { get; private set; }

    /// <summary>
    /// Approximate events per second based on recent activity.
    /// </summary>
    public double AppendsPerSecond
    {
        get
        {
            var elapsed = DateTime.UtcNow - LastAppendTime;
            return elapsed.TotalSeconds < 1 ? _totalAppended : Math.Max(0, _totalAppended / elapsed.TotalSeconds);
        }
    }

    internal void RecordAppend()
    {
        _ = Interlocked.Increment(ref _totalAppended);
        LastAppendTime = DateTime.UtcNow;
    }

    internal void RecordDiscard()
    {
        _ = Interlocked.Increment(ref _totalDiscarded);
    }

    /// <summary>
    /// Increments the total added counter by 1.
    /// </summary>
    internal void IncrementTotalAdded()
    {
        _ = Interlocked.Increment(ref _totalAppended);
        LastAppendTime = DateTime.UtcNow;
    }

    /// <summary>
    /// Increments the total added counter by the specified amount.
    /// </summary>
    internal void IncrementTotalAdded(int count)
    {
        _ = Interlocked.Add(ref _totalAppended, count);
        LastAppendTime = DateTime.UtcNow;
    }
    /// <summary>
    /// Increments the overwritten counter.
    /// </summary>
    internal void IncrementOverwritten()
    {
        _ = Interlocked.Increment(ref _totalDiscarded);
    }

    internal void Reset()
    {
        Volatile.Write(ref _totalAppended, 0);
        Volatile.Write(ref _totalDiscarded, 0);
        LastAppendTime = default;
    }
}

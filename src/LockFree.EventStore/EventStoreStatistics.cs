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

    /// <summary>
    /// Atomically increments the total appended-events counter and updates <see cref="LastAppendTime"/> to the current UTC time.
    /// </summary>
    /// <remarks>
    /// Safe for concurrent use; the counter is updated with an atomic operation so multiple threads may call this method concurrently.
    /// </remarks>
    internal void RecordAppend()
    {
        _ = Interlocked.Increment(ref _totalAppended);
        LastAppendTime = DateTime.UtcNow;
    }

    /// <summary>
    /// Atomically increments the count of events discarded due to capacity limits.
    /// </summary>
    internal void RecordDiscard()
    {
        _ = Interlocked.Increment(ref _totalDiscarded);
    }

    /// <summary>
    /// Increments the total added counter by 1.
    /// <summary>
    /// Atomically increments the total-appended counter by one and updates <see cref="LastAppendTime"/> to the current UTC time.
    /// </summary>
    /// <remarks>
    /// This method is thread-safe and uses an atomic increment to update the internal counter.
    /// </remarks>
    internal void IncrementTotalAdded()
    {
        _ = Interlocked.Increment(ref _totalAppended);
        LastAppendTime = DateTime.UtcNow;
    }

    /// <summary>
    /// Increments the total added counter by the specified amount.
    /// <summary>
    /// Atomically increases the total appended count by the specified amount and updates LastAppendTime to the current UTC time.
    /// </summary>
    /// <param name="count">Number of appended items to add to the total.</param>
    internal void IncrementTotalAdded(int count)
    {
        _ = Interlocked.Add(ref _totalAppended, count);
        LastAppendTime = DateTime.UtcNow;
    }
    /// <summary>
    /// Increments the overwritten counter.
    /// <summary>
    /// Atomically increments the counter of discarded (overwritten) events by one.
    /// </summary>
    internal void IncrementOverwritten()
    {
        _ = Interlocked.Increment(ref _totalDiscarded);
    }

    /// <summary>
    /// Resets the accumulated statistics for this EventStoreStatistics instance.
    /// </summary>
    /// <remarks>
    /// Sets the total appended and total discarded counters to zero and clears the last append timestamp.
    /// The counter writes are performed using volatile semantics to make the reset visible across threads.
    /// </remarks>
    internal void Reset()
    {
        Volatile.Write(ref _totalAppended, 0);
        Volatile.Write(ref _totalDiscarded, 0);
        LastAppendTime = default;
    }
}

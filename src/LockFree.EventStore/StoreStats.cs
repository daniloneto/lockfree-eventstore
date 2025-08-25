namespace LockFree.EventStore;

/// <summary>
/// Immutable struct containing telemetry statistics for an EventStore instance.
/// Provides insights into append operations, dropped events, snapshot exposure, and window advancement.
/// </summary>
/// <remarks>
/// Creates a new instance of StoreStats with the specified values.
/// </remarks>
public readonly struct StoreStats(long appendCount, long droppedCount, long snapshotBytesExposed, long windowAdvanceCount)
{
    /// <summary>
    /// Total number of successful append operations since store creation.
    /// </summary>
    public long AppendCount { get; init; } = appendCount;

    /// <summary>
    /// Total number of events that were dropped/discarded due to capacity limits.
    /// </summary>
    public long DroppedCount { get; init; } = droppedCount;

    /// <summary>
    /// Total number of bytes exposed through snapshot operations.
    /// Calculated as sum of Length * sizeof(Event) for each snapshot.
    /// </summary>
    public long SnapshotBytesExposed { get; init; } = snapshotBytesExposed;

    /// <summary>
    /// Total number of times the window head index was advanced across all partitions.
    /// Indicates how frequently sliding window boundaries are updated.
    /// </summary>
    public long WindowAdvanceCount { get; init; } = windowAdvanceCount;

    /// <summary>
    /// Returns a string representation of the store statistics.
    /// </summary>
    public override string ToString()
    {
        return $"StoreStats(Appends: {AppendCount}, Dropped: {DroppedCount}, SnapshotBytes: {SnapshotBytesExposed}, WindowAdvances: {WindowAdvanceCount})";
    }
}

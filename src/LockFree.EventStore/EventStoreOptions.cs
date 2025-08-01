namespace LockFree.EventStore;

/// <summary>
/// Configuration for <see cref="EventStore{TEvent}"/>.
/// </summary>
public sealed class EventStoreOptions<TEvent>
{
    /// <summary>
    /// Number of slots per partition. Defaults to 100_000.
    /// </summary>
    public int CapacityPerPartition { get; init; } = 100_000;

    /// <summary>
    /// Number of partitions. Defaults to <see cref="Environment.ProcessorCount"/>.
    /// </summary>
    public int Partitions { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Optional timestamp selector used for temporal queries.
    /// </summary>
    public IEventTimestampSelector<TEvent>? TimestampSelector { get; init; }
}

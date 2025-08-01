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
    /// Total capacity across all partitions. When set, takes precedence over CapacityPerPartition.
    /// </summary>
    public int? Capacity
    {
        get => _capacity;
        init
        {
            _capacity = value;
            if (value.HasValue)
                CapacityPerPartition = Math.Max(1, value.Value / Partitions);
        }
    }
    private readonly int? _capacity;

    /// <summary>
    /// Optional timestamp selector used for temporal queries.
    /// </summary>
    public IEventTimestampSelector<TEvent>? TimestampSelector { get; init; }

    /// <summary>
    /// Optional callback invoked when an event is discarded due to capacity limits.
    /// </summary>
    public Action<TEvent>? OnEventDiscarded { get; init; }

    /// <summary>
    /// Optional callback invoked when the store reaches capacity.
    /// </summary>
    public Action? OnCapacityReached { get; init; }

    /// <summary>
    /// Gets the effective total capacity.
    /// </summary>
    public int GetTotalCapacity() => _capacity ?? (CapacityPerPartition * Partitions);
}

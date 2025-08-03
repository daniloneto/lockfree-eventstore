using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Specialization of EventStoreOptions for the high-performance Event struct.
/// </summary>
public sealed class EventStoreOptions
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
    /// Optional callback invoked when an event is discarded due to capacity limits.
    /// </summary>
    public Action<Event>? OnEventDiscarded { get; init; }

    /// <summary>
    /// Optional callback invoked when the store reaches capacity.
    /// </summary>
    public Action? OnCapacityReached { get; init; }

    /// <summary>
    /// Gets the effective total capacity.
    /// </summary>
    public int GetEffectiveCapacity()
    {
        return CapacityPerPartition * Partitions;
    }
    
    /// <summary>
    /// Gets a default instance of EventTimestampSelector.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static EventTimestampSelector GetDefaultTimestampSelector() => new();
}

namespace LockFree.EventStore;

/// <summary>
/// Fluent builder for creating EventStore instances.
/// </summary>
public sealed class EventStoreBuilder<TEvent>
{
    private int? _capacity;
    private int? _partitions;
    private Action<TEvent>? _onEventDiscarded;
    private Action? _onCapacityReached;
    private IEventTimestampSelector<TEvent>? _timestampSelector;

    /// <summary>
    /// Sets the total capacity across all partitions.
    /// </summary>
    public EventStoreBuilder<TEvent> WithCapacity(int capacity)
    {
        _capacity = capacity;
        return this;
    }

    /// <summary>
    /// Sets the number of partitions.
    /// </summary>
    public EventStoreBuilder<TEvent> WithPartitions(int partitions)
    {
        _partitions = partitions;
        return this;
    }

    /// <summary>
    /// Sets a callback for when events are discarded.
    /// </summary>
    public EventStoreBuilder<TEvent> OnDiscarded(Action<TEvent> callback)
    {
        _onEventDiscarded = callback;
        return this;
    }

    /// <summary>
    /// Sets a callback for when capacity is reached.
    /// </summary>
    public EventStoreBuilder<TEvent> OnCapacityReached(Action callback)
    {
        _onCapacityReached = callback;
        return this;
    }

    /// <summary>
    /// Sets the timestamp selector for temporal queries.
    /// </summary>
    public EventStoreBuilder<TEvent> WithTimestampSelector(IEventTimestampSelector<TEvent> selector)
    {
        _timestampSelector = selector;
        return this;
    }

    /// <summary>
    /// Creates the EventStore with the configured options.
    /// </summary>
    public EventStore<TEvent> Create()
    {
        var options = new EventStoreOptions<TEvent>
        {
            Capacity = _capacity,
            Partitions = _partitions ?? Environment.ProcessorCount,
            OnEventDiscarded = _onEventDiscarded,
            OnCapacityReached = _onCapacityReached,
            TimestampSelector = _timestampSelector
        };

        return new EventStore<TEvent>(options);
    }
}

/// <summary>
/// Static factory methods for EventStore creation.
/// </summary>
public static class EventStore
{
    /// <summary>
    /// Creates a new fluent builder for EventStore&lt;TEvent&gt;.
    /// </summary>
    public static EventStoreBuilder<TEvent> For<TEvent>()
    {
        return new EventStoreBuilder<TEvent>();
    }
}

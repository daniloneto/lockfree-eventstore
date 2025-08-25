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
    private bool? _enableWindowTracking; // RFC 002: expose runtime flag

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
    /// <summary>
    /// Sets the timestamp selector used to extract an event's timestamp for time-based operations.
    /// </summary>
    /// <param name="selector">An implementation of <see cref="IEventTimestampSelector{TEvent}"/> used to determine each event's timestamp.</param>
    /// <returns>The current <see cref="EventStoreBuilder{TEvent}"/> instance for fluent configuration.</returns>
    public EventStoreBuilder<TEvent> WithTimestampSelector(IEventTimestampSelector<TEvent> selector)
    {
        _timestampSelector = selector;
        return this;
    }

    /// <summary>
    /// Enables or disables runtime window tracking (bucket maintenance on append).
    /// When disabled, appends bypass all window/bucket logic and time-filtered window queries will throw.
    /// Default is enabled.
    /// <summary>
    /// Sets whether runtime window tracking (bucket maintenance on append) is enabled.
    /// </summary>
    /// <remarks>
    /// When enabled (default), the store maintains runtime windows/buckets on append to support time-filtered window queries.
    /// When disabled, appends bypass window/bucket maintenance and time-filtered window queries will throw.
    /// </remarks>
    /// <param name="enabled">True to enable window tracking; false to disable it.</param>
    /// <returns>The current <see cref="EventStoreBuilder{TEvent}"/> instance for fluent configuration.</returns>
    public EventStoreBuilder<TEvent> WithEnableWindowTracking(bool enabled)
    {
        _enableWindowTracking = enabled;
        return this;
    }

    /// <summary>
    /// Creates the EventStore with the configured options.
    /// <summary>
    /// Builds and returns an EventStore{TEvent} configured from the builder's fluent settings.
    /// </summary>
    /// <returns>A new EventStore{TEvent} instance configured with the builder's options. Partitions default to <c>Environment.ProcessorCount</c> if not specified; window tracking defaults to enabled.</returns>
    public EventStore<TEvent> Create()
    {
        var options = new EventStoreOptions<TEvent>
        {
            Capacity = _capacity,
            Partitions = _partitions ?? Environment.ProcessorCount,
            OnEventDiscarded = _onEventDiscarded,
            OnCapacityReached = _onCapacityReached,
            TimestampSelector = _timestampSelector,
            EnableWindowTracking = _enableWindowTracking ?? true
        };

        options.Validate();
        return new EventStore<TEvent>(options);
    }
}

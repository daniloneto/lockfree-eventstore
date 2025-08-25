using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Specialized high-performance EventStore for Event structs using contiguous Event[] arrays.
/// Uses Structure of Arrays (SoA) approach for optimal cache locality and zero-allocation operations.
/// </summary>
public sealed class SpecializedEventStore
{
    private readonly LockFreeEventRingBuffer[] _partitions;
    private readonly KeyMap _keyMap; // Hot path optimization

    // Window state per partition for incremental aggregation
    private readonly PartitionWindowState[] _windowStates;

    /// <summary>
    /// Initializes a new SpecializedEventStore with default configuration.
    /// </summary>
    public SpecializedEventStore() : this(1024, 4) { }

    /// <summary>
    /// Initializes a new SpecializedEventStore with specified capacity.
    /// </summary>
    public SpecializedEventStore(int capacity) : this(capacity, 4) { }

    /// <summary>
    /// Initializes a new SpecializedEventStore with specified capacity and partitions.
    /// <summary>
    /// Initializes a new SpecializedEventStore with the specified total capacity and partition count.
    /// </summary>
    /// <param name="capacity">Total target capacity for the store; will be divided across partitions (per-partition capacity = max(1, capacity / partitions)).</param>
    /// <param name="partitions">Number of partitions (ring buffers) to create.</param>
    /// <param name="onEventDiscarded">Optional callback invoked when an event is discarded; if provided, the store wires an internal discard handler that updates store statistics before invoking this callback.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="capacity"/> or <paramref name="partitions"/> is less than or equal to zero.</exception>
    public SpecializedEventStore(int capacity, int partitions, Action<Event>? onEventDiscarded = null)
    {
        // CA1512: prefer guard methods
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);

        Statistics = new EventStoreStatistics();
        _keyMap = new KeyMap();
        _partitions = new LockFreeEventRingBuffer[partitions];

        var capacityPerPartition = Math.Max(1, capacity / partitions);
        for (var i = 0; i < partitions; i++)
        {
            _partitions[i] = new LockFreeEventRingBuffer(
                capacityPerPartition,
                onEventDiscarded != null ? OnEventDiscardedInternal : null);
        }

        _windowStates = new PartitionWindowState[partitions];
        for (var i = 0; i < _windowStates.Length; i++)
        {
            _windowStates[i].Reset();
        }
    }

    /// <summary>
    /// Number of partitions.
    /// </summary>
    public int Partitions => _partitions.Length;

    /// <summary>
    /// Total configured capacity across all partitions.
    /// </summary>
    public int Capacity
    {
        get
        {
            var total = 0;
            for (var i = 0; i < _partitions.Length; i++)
            {
                total += _partitions[i].Capacity;
            }
            return total;
        }
    }

    /// <summary>
    /// Approximate total number of events across partitions.
    /// </summary>
    public long CountApprox
    {
        get
        {
            var total = 0L;
            foreach (var p in _partitions)
            {
                total += p.CountApprox;
            }
            return total;
        }
    }

    /// <summary>
    /// Whether the store is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Gets the statistics for this event store.
    /// </summary>
    public EventStoreStatistics Statistics { get; }

    // --- String <-> KeyId bridge (uses _keyMap) ---

    /// <summary>
    /// Resolves or creates a KeyId for the provided string key.
    /// <summary>
    /// Gets the canonical KeyId for the given string key, creating and registering a new KeyId if none exists.
    /// </summary>
    /// <param name="key">The string key to resolve or register.</param>
    /// <returns>The existing or newly created <see cref="KeyId"/> associated with the key.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public KeyId GetOrCreateKeyId(string key)
    {
        return _keyMap.GetOrAdd(key);
    }

    /// <summary>
    /// Attempts to resolve a KeyId for the provided string key without creating a new one.
    /// <summary>
    /// Attempts to resolve an existing KeyId for the specified string key without creating a new mapping.
    /// </summary>
    /// <param name="key">The string key to look up.</param>
    /// <param name="id">When the method returns, contains the associated KeyId if found; otherwise the default KeyId.</param>
    /// <returns>True if a mapping was found and <paramref name="id"/> was set; otherwise false.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetKeyId(string key, out KeyId id)
    {
        return _keyMap.TryGet(key, out id);
    }

    /// <summary>
    /// Number of distinct keys registered in the key map.
    /// </summary>
    public int RegisteredKeysCount => _keyMap.Count;

    /// <summary>
    /// Returns a snapshot of all string->KeyId mappings.
    /// <summary>
    /// Returns a snapshot of all registered string keys mapped to their corresponding <see cref="KeyId"/> values.
    /// </summary>
    /// <returns>
    /// A read-only dictionary containing the current string-to-<see cref="KeyId"/> mappings. The returned dictionary is a snapshot and will not reflect keys added after the call.
    /// </returns>
    public IReadOnlyDictionary<string, KeyId> GetKeyMappings()
    {
        return _keyMap.GetAllMappings();
    }

    /// <summary>
    /// Adds a single event by string key, value and timestamp ticks.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(string key, double value, long timestampTicks)
    {
        var keyId = _keyMap.GetOrAdd(key);
        Add(keyId, value, timestampTicks);
    }

    /// <summary>
    /// Adds a single event by string key, value and DateTime timestamp.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(string key, double value, DateTime timestamp)
    {
        var keyId = _keyMap.GetOrAdd(key);
        Add(keyId, value, timestamp);
    }

    /// <summary>
    /// Queries events by string key within an optional time range.
    /// If the key was never seen, returns an empty sequence.
    /// <summary>
    /// Returns all events for the specified string key, optionally constrained to an inclusive time range.
    /// </summary>
    /// <param name="key">The string key whose events to retrieve.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps; if null, no lower bound is applied.</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps; if null, no upper bound is applied.</param>
    /// <returns>A sequence of <see cref="Event"/> matching the key and time bounds. Returns an empty sequence if the key is not registered.</returns>
    /// <remarks>
    /// This method is obsolete. Use <see cref="QueryByKeyZeroAlloc(KeyId,Action{ReadOnlySpan{Event}},DateTime?,DateTime?)"/> for zero-allocation streaming.
    /// </remarks>
    [Obsolete("Use QueryByKeyZeroAlloc(key, processor, from, to) for zero-allocation streaming. See new_feature.md.", DiagnosticId = "LF0001", UrlFormat = "https://github.com/daniloneto/lockfree-eventstore/blob/main/new_feature.md#compatibilidade-e-obsolesc%C3%AAncia")]
    public IEnumerable<Event> Query(string key, DateTime? from = null, DateTime? to = null)
    {
        return _keyMap.TryGet(key, out var keyId)
            ? Query(keyId, from, to)
            : [];
    }

    /// <summary>
    /// Zero-allocation query by string key using chunked processing.
    /// If the key was never seen, nothing is emitted.
    /// </summary>
    public void QueryByKeyZeroAlloc(string key, Action<ReadOnlySpan<Event>> processor, DateTime? from = null, DateTime? to = null, int chunkSize = Buffers.DefaultChunkSize)
    {
        if (_keyMap.TryGet(key, out var keyId))
        {
            QueryByKeyZeroAlloc(keyId, processor, from, to, chunkSize);
        }
    }

    /// <summary>
    /// Adds a single event to the store with optimized partitioning.
    /// <summary>
    /// Appends a single event to the store by enqueuing it into the partition determined from the event's key.
    /// </summary>
    /// <param name="e">The event to append. May be discarded if the target partition's buffer is full.</param>
    /// <remarks>
    /// The method updates internal append statistics. It will not block; enqueue failures (due to capacity) are ignored here and are accounted for via the store's discard handling.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(Event e)
    {
        var partition = GetPartition(e.Key);
        _ = _partitions[partition].TryEnqueue(e);
        Statistics.RecordAppend();
    }

    /// <summary>
    /// Adds a single event by key, value, and timestamp.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(KeyId key, double value, long timestampTicks)
    {
        Add(new Event(key, value, timestampTicks));
    }

    /// <summary>
    /// Adds a single event by key, value, and DateTime.
    /// <summary>
    /// Adds an event with the specified key, value, and timestamp to the store.
    /// </summary>
    /// <param name="key">Identifier for the event's key (KeyId).</param>
    /// <param name="value">Numeric value of the event.</param>
    /// <param name="timestamp">Event timestamp; the DateTime is converted to ticks for storage.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(KeyId key, double value, DateTime timestamp)
    {
        Add(new Event(key, value, timestamp.Ticks));
    }

    /// <summary>
    /// Adds multiple events efficiently using batch operations grouped by partition.
    /// Uses pooled buffers to avoid allocations in hot path.
    /// <summary>
    /// Adds a contiguous span of events to the store in a single, partition-aware batch operation.
    /// </summary>
    /// <remarks>
    /// Distributes the supplied events to per-partition pooled buffers, enqueues each partition's
    /// buffer in a single batch operation, and increments the store's add statistics. Uses rented
    /// buffers from the shared Buffers pool to avoid allocations and returns them in a finally
    /// block. Returns immediately if the input span is empty. The method does not return a value;
    /// any per-partition enqueue result is intentionally discarded.
    /// </remarks>
    public void AddRange(ReadOnlySpan<Event> events)
    {
        if (events.IsEmpty)
        {
            return;
        }

        // Use pooled arrays for partition grouping to avoid allocations
        var partitionBuffers = new Event[_partitions.Length][];
        var partitionCounts = Buffers.RentInts(_partitions.Length);

        try
        {
            // Initialize partition buffers and counts
            for (var i = 0; i < _partitions.Length; i++)
            {
                partitionBuffers[i] = Buffers.RentEvents(Math.Max(16, events.Length / _partitions.Length));
                partitionCounts[i] = 0;
            }

            // Distribute events to partitions
            foreach (var e in events)
            {
                var partition = GetPartition(e.Key);
                var count = partitionCounts[partition];

                // Expand buffer if needed
                if (count >= partitionBuffers[partition].Length)
                {
                    var oldBuffer = partitionBuffers[partition];
                    var newBuffer = Buffers.RentEvents(count * 2);
                    oldBuffer.AsSpan(0, count).CopyTo(newBuffer);
                    Buffers.ReturnEvents(oldBuffer);
                    partitionBuffers[partition] = newBuffer;
                }

                partitionBuffers[partition][count] = e;
                partitionCounts[partition] = count + 1;
            }

            // Add to each partition in batch for optimal cache usage
            for (var i = 0; i < _partitions.Length; i++)
            {
                var count = partitionCounts[i];
                if (count > 0)
                {
                    var span = partitionBuffers[i].AsSpan(0, count);
                    _ = _partitions[i].TryEnqueueBatch(span);
                }
            }

            Statistics.IncrementTotalAdded(events.Length);
        }
        finally
        {
            // Return all buffers to pools
            Buffers.ReturnInts(partitionCounts);
            for (var i = 0; i < _partitions.Length; i++)
            {
                if (partitionBuffers[i] != null)
                {
                    Buffers.ReturnEvents(partitionBuffers[i]);
                }
            }
        }
    }

    /// <summary>
    /// Adds multiple events from an enumerable.
    /// Uses chunked processing to avoid large allocations.
    /// <summary>
    /// Appends a sequence of events to the store, processing the source efficiently without allocating for common collection types.
    /// </summary>
    /// <param name="events">The events to add. If <see cref="Event[]"/> or <see cref="List{Event}"/>, the implementation uses spans for zero-copy processing; otherwise events are consumed in pooled chunks to minimize allocations.</param>
    public void AddRange(IEnumerable<Event> events)
    {
        if (events is Event[] array)
        {
            AddRange(array.AsSpan());
        }
        else if (events is List<Event> list)
        {
            AddRange(CollectionsMarshal.AsSpan(list));
        }
        else
        {
            // Process in chunks to avoid large temporary allocations
            Buffers.WithRentedBuffer(Buffers.DefaultChunkSize, buffer =>
            {
                using var enumerator = events.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    var count = 0;
                    do
                    {
                        buffer[count++] = enumerator.Current;
                    }
                    while (count < buffer.Length && enumerator.MoveNext());

                    // Process the chunk
                    AddRange(buffer.AsSpan(0, count));
                }
            }, Buffers.EventPool);
        }
    }

    /// <summary>
    /// Gets all events in the store (snapshot at call time).
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot()
    {
        var results = new List<Event>();
        foreach (var partition in _partitions)
        {
            results.AddRange(partition.EnumerateSnapshot());
        }
        return results;
    }

    /// <summary>
    /// Queries events by key within a time range.
    /// <summary>
    /// Returns a snapshot enumeration of events for the specified KeyId within the optional time range (inclusive).
    /// </summary>
    /// <param name="key">KeyId identifying the event stream to query.</param>
    /// <param name="from">Optional lower bound (inclusive) of event timestamps; when null there is no lower bound.</param>
    /// <param name="to">Optional upper bound (inclusive) of event timestamps; when null there is no upper bound.</param>
    /// <returns>An <see cref="IEnumerable{Event}"/> of events from the partition that contains <paramref name="key"/>, filtered by the provided time bounds.</returns>
    /// <remarks>
    /// This method is obsolete; prefer <c>QueryByKeyZeroAlloc(key, processor, from, to)</c> for zero-allocation streaming.
    /// </remarks>
    [Obsolete("Use QueryByKeyZeroAlloc(key, processor, from, to) for zero-allocation streaming. See new_feature.md.", DiagnosticId = "LF0001", UrlFormat = "https://github.com/daniloneto/lockfree-eventstore/blob/main/new_feature.md#compatibilidade-e-obsolesc%C3%AAncia")]
    public IEnumerable<Event> Query(KeyId key, DateTime? from = null, DateTime? to = null)
    {
        var partition = GetPartition(key);
        var partitionBuffer = _partitions[partition];

        return from.HasValue && to.HasValue
            ? partitionBuffer.EnumerateSnapshot(key, from.Value.Ticks, to.Value.Ticks)
            : from.HasValue
            ? partitionBuffer.EnumerateSnapshot(e => e.Key.Equals(key) && e.TimestampTicks >= from.Value.Ticks)
            : to.HasValue
                ? partitionBuffer.EnumerateSnapshot(e => e.Key.Equals(key) && e.TimestampTicks <= to.Value.Ticks)
                : partitionBuffer.EnumerateSnapshot(key);
    }

    /// <summary>
    /// Queries events across all partitions within a time range.
    /// <summary>
    /// Returns a materialized snapshot of all events across all partitions, optionally filtered by a time range.
    /// </summary>
    /// <remarks>
    /// This method is obsolete — prefer <see cref="QueryZeroAlloc(Action{ReadOnlySpan{Event}}, DateTime?, DateTime?)"/> for zero-allocation streaming.
    /// The result is a newly allocated <see cref="List{Event}"/> aggregated from each partition at the time of the call.
    /// When both <paramref name="from"/> and <paramref name="to"/> are provided, events with TimestampTicks in the inclusive range [from, to] are returned.
    /// If only <paramref name="from"/> is provided, events with TimestampTicks >= from are returned; if only <paramref name="to"/> is provided, events with TimestampTicks <= to are returned.
    /// If neither is provided, all events from all partitions are returned.
    /// </remarks>
    /// <param name="from">Optional lower bound (inclusive) of the event timestamp filter.</param>
    /// <param name="to">Optional upper bound (inclusive) of the event timestamp filter.</param>
    /// <returns>An IEnumerable&lt;Event&gt; containing the matching events as a materialized list.</returns>
    [Obsolete("Use QueryZeroAlloc(processor, from, to) for zero-allocation streaming. See new_feature.md.", DiagnosticId = "LF0001", UrlFormat = "https://github.com/daniloneto/lockfree-eventstore/blob/main/new_feature.md#compatibilidade-e-obsolesc%C3%AAncia")]
    public IEnumerable<Event> Query(DateTime? from = null, DateTime? to = null)
    {
        var results = new List<Event>();

        if (from.HasValue && to.HasValue)
        {
            var fromTicks = from.Value.Ticks;
            var toTicks = to.Value.Ticks;
            foreach (var partition in _partitions)
            {
                results.AddRange(partition.EnumerateSnapshot(fromTicks, toTicks));
            }
        }
        else if (from.HasValue)
        {
            var fromTicks = from.Value.Ticks;
            foreach (var partition in _partitions)
            {
                results.AddRange(partition.EnumerateSnapshot(e => e.TimestampTicks >= fromTicks));
            }
        }
        else if (to.HasValue)
        {
            var toTicks = to.Value.Ticks;
            foreach (var partition in _partitions)
            {
                results.AddRange(partition.EnumerateSnapshot(e => e.TimestampTicks <= toTicks));
            }
        }
        else
        {
            foreach (var partition in _partitions)
            {
                results.AddRange(partition.EnumerateSnapshot());
            }
        }

        return results;
    }

    /// <summary>
    /// Purges events older than the specified date from all partitions.
    /// <summary>
    /// Removes all events strictly older than the specified timestamp from every partition.
    /// </summary>
    /// <param name="olderThan">Events with timestamps less than this DateTime are removed (strictly older).</param>
    /// <returns>The total number of events removed across all partitions.</returns>
    public long Purge(DateTime olderThan)
    {
        var totalPurged = 0L;
        var thresholdTicks = olderThan.Ticks;

        foreach (var partition in _partitions)
        {
            totalPurged += partition.Purge(thresholdTicks);
        }

        return totalPurged;
    }

    /// <summary>
    /// Gets partition statistics for monitoring and debugging.
    /// <summary>
    /// Returns a snapshot of per-partition ring-buffer statistics.
    /// </summary>
    /// <returns>
    /// An enumerable of tuples, one per partition, containing:
    /// (PartitionIndex, Head, Tail, Epoch, Count). The sequence is ordered by partition index;
    /// values reflect each partition's statistics at the time they are read.
    /// </returns>
    public IEnumerable<(int PartitionIndex, long Head, long Tail, int Epoch, long Count)> GetPartitionStatistics()
    {
        for (var i = 0; i < _partitions.Length; i++)
        {
            var (head, tail, epoch, count) = _partitions[i].GetStatistics();
            yield return (i, head, tail, epoch, count);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetPartition(KeyId key)
    {
        return Partitioners.ForKeyIdSimple(key, _partitions.Length);
    }

    /// <summary>
    /// Handles an event that was dropped from the store by recording the discard in Statistics.
    /// </summary>
    /// <param name="discardedEvent">The event instance that was discarded; provided for any additional handling implemented later.</param>
    private void OnEventDiscardedInternal(Event discardedEvent)
    {
        Statistics.RecordDiscard();
        // Additional logic for discarded events can be added here
    }

    /// <summary>
    /// Aggregates values by key across all partitions.
    /// <summary>
    /// Aggregates the sum of Event.Value for each KeyId across all partitions and returns a mapping KeyId → sum.
    /// </summary>
    /// <returns>
    /// A new Dictionary where each key is a KeyId present in the store and the value is the sum of all Event.Value entries for that key
    /// as observed by each partition's snapshot at the time of enumeration.
    /// </returns>
    public Dictionary<KeyId, double> AggregateByKey()
    {
        var results = new Dictionary<KeyId, double>();

        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
            {
                results[e.Key] = results.TryGetValue(e.Key, out var existingValue) ? existingValue + e.Value : e.Value;
            }
        }

        return results;
    }

    /// <summary>
    /// Aggregates values by key within a time range.
    /// <summary>
    /// Aggregates event values by KeyId for events whose timestamps fall within the specified time window.
    /// </summary>
    /// <param name="from">Start of the time window (inclusive).</param>
    /// <param name="to">End of the time window (inclusive).</param>
    /// <returns>
    /// A dictionary mapping each KeyId to the sum of its event values observed between <paramref name="from"/> and <paramref name="to"/>.
    /// Returns an empty dictionary if no events are found in the window.
    /// </returns>
    public Dictionary<KeyId, double> AggregateByKey(DateTime from, DateTime to)
    {
        var results = new Dictionary<KeyId, double>();
        var fromTicks = from.Ticks;
        var toTicks = to.Ticks;

        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot(fromTicks, toTicks))
            {
                results[e.Key] = results.TryGetValue(e.Key, out var existingValue) ? existingValue + e.Value : e.Value;
            }
        }

        return results;
    }

    /// <summary>
    /// Gets the latest value for each key.
    /// <summary>
    /// Builds a snapshot of the most-recent event value for each KeyId across all partitions.
    /// </summary>
    /// <returns>
    /// A dictionary mapping each KeyId to a tuple of (Value, Timestamp). For each key the entry contains
    /// the event with the highest TimestampTicks observed at call time; the Timestamp is constructed
    /// from the event's ticks as a <see cref="DateTime"/>.
    /// </returns>
    public Dictionary<KeyId, (double Value, DateTime Timestamp)> GetLatestValues()
    {
        var results = new Dictionary<KeyId, (double Value, DateTime Timestamp)>();

        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
            {
                var timestamp = new DateTime(e.TimestampTicks);
                if (!results.TryGetValue(e.Key, out var existing) || e.TimestampTicks > existing.Timestamp.Ticks)
                {
                    results[e.Key] = (e.Value, timestamp);
                }
            }
        }

        return results;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// Processes results in fixed-size chunks to avoid large allocations.
    /// <summary>
    /// Streams a point-in-time snapshot of all events, invoking <paramref name="processor"/> with contiguous, pooled chunks of events.
    /// </summary>
    /// <param name="processor">Action invoked for each chunk; receives a ReadOnlySpan&lt;Event&gt; containing up to <paramref name="chunkSize"/> events. Calls are made synchronously on the calling thread.</param>
    /// <param name="chunkSize">Maximum number of events provided to <paramref name="processor"/> per invocation. Defaults to <c>Buffers.DefaultChunkSize</c>. Must be &gt; 0.</param>
    /// <remarks>
    /// - The snapshot is taken partition-by-partition; ordering is preserved within each partition but not globally across partitions.
    /// - Buffers are rented and returned to pools; the span passed to <paramref name="processor"/> is only valid for the duration of that call and must not be retained.
    /// </remarks>
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        Buffers.WithRentedBuffer(chunkSize, buffer =>
        {
            foreach (var partition in _partitions)
            {
                var partitionBuffer = Buffers.RentEvents(partition.Capacity);
                try
                {
                    var count = 0;
                    foreach (var evt in partition.EnumerateSnapshot())
                    {
                        partitionBuffer[count++] = evt;

                        if (count >= chunkSize)
                        {
                            processor(partitionBuffer.AsSpan(0, count));
                            count = 0;
                        }
                    }

                    // Process remaining events
                    if (count > 0)
                    {
                        processor(partitionBuffer.AsSpan(0, count));
                    }
                }
                finally
                {
                    Buffers.ReturnEvents(partitionBuffer);
                }
            }
        }, Buffers.EventPool);
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// </summary>
    public void EnumerateSnapshotZeroAlloc(Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        foreach (var partition in _partitions)
        {
            partition.SnapshotZeroAlloc(processor, chunkSize);
        }
    }

    /// <summary>
    /// Zero-allocation query by key using chunked processing.
    /// <summary>
    /// Streams events for a single KeyId to the provided processor in zero-allocation, chunked slices.
    /// </summary>
    /// <remarks>
    /// The method locates the partition for <paramref name="key"/> and invokes a partition-level
    /// zero-allocation snapshot that calls <paramref name="processor"/> with ReadOnlySpan&lt;Event&gt;
    /// segments that match the key and optional time bounds. Time bounds, when supplied, are inclusive.
    /// This is a streaming, allocation-minimizing API; the processor must not retain references to the span after it returns.
    /// </remarks>
    /// <param name="key">The KeyId whose events should be streamed.</param>
    /// <param name="processor">Action invoked with each chunk of matching events.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps.</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps.</param>
    /// <param name="chunkSize">Maximum chunk size passed to the processor (defaults to Buffers.DefaultChunkSize).</param>
    public void QueryByKeyZeroAlloc(KeyId key, Action<ReadOnlySpan<Event>> processor, DateTime? from = null, DateTime? to = null, int chunkSize = Buffers.DefaultChunkSize)
    {
        var partition = GetPartition(key);
        var partitionBuffer = _partitions[partition];

        if (from.HasValue && to.HasValue)
        {
            partitionBuffer.SnapshotByKeyAndTimeZeroAlloc(key, from.Value.Ticks, to.Value.Ticks, processor, chunkSize);
        }
        else if (from.HasValue)
        {
            var fromTicks = from.Value.Ticks;
            partitionBuffer.SnapshotFilteredZeroAlloc(e => e.Key.Equals(key) && e.TimestampTicks >= fromTicks, processor, chunkSize);
        }
        else if (to.HasValue)
        {
            var toTicks = to.Value.Ticks;
            partitionBuffer.SnapshotFilteredZeroAlloc(e => e.Key.Equals(key) && e.TimestampTicks <= toTicks, processor, chunkSize);
        }
        else
        {
            partitionBuffer.SnapshotByKeyZeroAlloc(key, processor, chunkSize);
        }
    }

    /// <summary>
    /// Zero-allocation query across all partitions using chunked processing.
    /// </summary>
    public void QueryZeroAlloc(Action<ReadOnlySpan<Event>> processor, DateTime? from = null, DateTime? to = null, int chunkSize = Buffers.DefaultChunkSize)
    {
        if (from.HasValue && to.HasValue)
        {
            var fromTicks = from.Value.Ticks;
            var toTicks = to.Value.Ticks;
            foreach (var partition in _partitions)
            {
                partition.SnapshotTimeRangeZeroAlloc(fromTicks, toTicks, processor, chunkSize);
            }
        }
        else if (from.HasValue)
        {
            var fromTicks = from.Value.Ticks;
            foreach (var partition in _partitions)
            {
                partition.SnapshotFilteredZeroAlloc(e => e.TimestampTicks >= fromTicks, processor, chunkSize);
            }
        }
        else if (to.HasValue)
        {
            var toTicks = to.Value.Ticks;
            foreach (var partition in _partitions)
            {
                partition.SnapshotFilteredZeroAlloc(e => e.TimestampTicks <= toTicks, processor, chunkSize);
            }
        }
        else
        {
            foreach (var partition in _partitions)
            {
                partition.SnapshotZeroAlloc(processor, chunkSize);
            }
        }
    }

    /// <summary>
    /// Zero-allocation aggregation by key using pooled dictionaries and buffers.
    /// </summary>
    public void AggregateByKeyZeroAlloc(Action<ReadOnlySpan<KeyValuePair<KeyId, double>>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var results = new Dictionary<KeyId, double>();

        // Process each partition and accumulate results
        foreach (var partition in _partitions)
        {
            partition.SnapshotZeroAlloc(events => AccumulateResults(results, events), chunkSize);
        }

        // Emit results in chunks
        EmitResultsChunks(results, processor, chunkSize);
    }

    /// <summary>
    /// Accumulates the values of a span of events into a dictionary keyed by KeyId.
    /// </summary>
    /// <param name="results">Dictionary that will be updated in-place: each event's Value is added to the entry for its Key (created if missing).</param>
    /// <param name="events">Span of events to accumulate.</param>
    /// <remarks>
    /// This mutates <paramref name="results"/> and is not thread-safe.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AccumulateResults(Dictionary<KeyId, double> results, ReadOnlySpan<Event> events)
    {
        for (var i = 0; i < events.Length; i++)
        {
            var e = events[i];
            results[e.Key] = results.TryGetValue(e.Key, out var existing) ? existing + e.Value : e.Value;
        }
    }

    /// <summary>
    /// Emits the accumulated key/value pairs in fixed-size chunks to the provided processor.
    /// </summary>
    /// <param name="results">Dictionary of aggregated results to emit. If empty, nothing is emitted.</param>
    /// <param name="processor">Callback invoked with each chunk as a <see cref="ReadOnlySpan{T}"/> of KeyValuePair&lt;KeyId,double&gt;.</param>
    /// <param name="chunkSize">Maximum number of items to include in each chunk passed to <paramref name="processor"/>.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EmitResultsChunks(Dictionary<KeyId, double> results, Action<ReadOnlySpan<KeyValuePair<KeyId, double>>> processor, int chunkSize)
    {
        if (results.Count == 0)
        {
            return;
        }

        Buffers.WithRentedBuffer(chunkSize, buffer =>
        {
            var count = 0;
            foreach (var kvp in results)
            {
                buffer[count++] = kvp;
                if (count >= buffer.Length)
                {
                    processor(buffer.AsSpan(0, count));
                    count = 0;
                }
            }
            if (count > 0)
            {
                processor(buffer.AsSpan(0, count));
            }
        }, ArrayPool<KeyValuePair<KeyId, double>>.Shared);
    }

    /// <summary>
    /// Zero-allocation time-filtered aggregation by key.
    /// <summary>
    /// Aggregates event values by key over a time window and delivers the aggregated results to the provided processor in fixed-size, zero-allocation chunks.
    /// </summary>
    /// <remarks>
    /// This method scans each partition for events whose timestamps fall between <paramref name="from"/> and <paramref name="to"/> (using the events' <c>Ticks</c>), accumulates the sum of values per <see cref="KeyId"/>, and then invokes <paramref name="processor"/> with the aggregated results in slices of up to <paramref name="chunkSize"/> items. The operation uses pooled buffers and zero-allocation snapshot APIs on partitions to minimize temporary allocations.
    /// </remarks>
    /// <param name="from">Start of the time window (inclusive lower bound) used to filter events.</param>
    /// <param name="to">End of the time window used to filter events.</param>
    /// <param name="processor">Callback invoked with each chunk of aggregated results as a <see cref="ReadOnlySpan{T}"/> of <see cref="KeyValuePair{KeyId,double}"/>.</param>
    /// <param name="chunkSize">Maximum number of aggregated entries passed to <paramref name="processor"/> in a single call. Must be a positive value; defaults to <see cref="Buffers.DefaultChunkSize"/>.</param>
    public void AggregateByKeyZeroAlloc(DateTime from, DateTime to, Action<ReadOnlySpan<KeyValuePair<KeyId, double>>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var results = new Dictionary<KeyId, double>();
        var fromTicks = from.Ticks;
        var toTicks = to.Ticks;

        foreach (var partition in _partitions)
        {
            partition.SnapshotTimeRangeZeroAlloc(fromTicks, toTicks, events => AccumulateResults(results, events), chunkSize);
        }

        EmitResultsChunks(results, processor, chunkSize);
    }
}

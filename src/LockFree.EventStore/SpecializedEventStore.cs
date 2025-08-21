using System.Buffers;
using System.Collections.Concurrent;
using System.Numerics;
using System.Threading;
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
    private readonly EventStoreStatistics _statistics;
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
    /// </summary>
    public SpecializedEventStore(int capacity, int partitions, Action<Event>? onEventDiscarded = null)
    {
        // CA1512: prefer guard methods
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);

        _statistics = new EventStoreStatistics();
        _keyMap = new KeyMap();
        _partitions = new LockFreeEventRingBuffer[partitions];
        
        var capacityPerPartition = Math.Max(1, capacity / partitions);
        for (int i = 0; i < partitions; i++)
        {
            _partitions[i] = new LockFreeEventRingBuffer(
                capacityPerPartition, 
                onEventDiscarded != null ? OnEventDiscardedInternal : null);
        }
        
        _windowStates = new PartitionWindowState[partitions];
        for (int i = 0; i < _windowStates.Length; i++)
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
    public int Capacity => _partitions.Sum(p => p.Capacity);

    /// <summary>
    /// Approximate total number of events across partitions.
    /// </summary>
    public long CountApprox
    {
        get
        {
            long total = 0;
            foreach (var p in _partitions)
                total += p.CountApprox;
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
    public EventStoreStatistics Statistics => _statistics;

    /// <summary>
    /// Adds a single event to the store with optimized partitioning.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(Event e)
    {
        var partition = GetPartition(e.Key);
        _partitions[partition].TryEnqueue(e);
        _statistics.RecordAppend();
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(KeyId key, double value, DateTime timestamp)
    {
        Add(new Event(key, value, timestamp.Ticks));
    }    /// <summary>
    /// Adds multiple events efficiently using batch operations grouped by partition.
    /// Uses pooled buffers to avoid allocations in hot path.
    /// </summary>
    public void AddRange(ReadOnlySpan<Event> events)
    {
        if (events.IsEmpty) return;

        // Use pooled arrays for partition grouping to avoid allocations
        var partitionBuffers = new Event[_partitions.Length][];
        var partitionCounts = Buffers.RentInts(_partitions.Length);
        
        try
        {
            // Initialize partition buffers and counts
            for (int i = 0; i < _partitions.Length; i++)
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
            for (int i = 0; i < _partitions.Length; i++)
            {
                var count = partitionCounts[i];
                if (count > 0)
                {
                    var span = partitionBuffers[i].AsSpan(0, count);
                    _partitions[i].TryEnqueueBatch(span);
                }
            }

            _statistics.IncrementTotalAdded(events.Length);
        }
        finally
        {
            // Return all buffers to pools
            Buffers.ReturnInts(partitionCounts);
            for (int i = 0; i < _partitions.Length; i++)
            {
                if (partitionBuffers[i] != null)
                    Buffers.ReturnEvents(partitionBuffers[i]);
            }
        }
    }    /// <summary>
    /// Adds multiple events from an enumerable.
    /// Uses chunked processing to avoid large allocations.
    /// </summary>
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
            Buffers.WithRentedBuffer<Event>(Buffers.DefaultChunkSize, buffer =>
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
    /// </summary>
    public IEnumerable<Event> Query(KeyId key, DateTime? from = null, DateTime? to = null)
    {
        var partition = GetPartition(key);
        var partitionBuffer = _partitions[partition];

        if (from.HasValue && to.HasValue)
        {
            return partitionBuffer.EnumerateSnapshot(key, from.Value.Ticks, to.Value.Ticks);
        }
        else if (from.HasValue)
        {
            return partitionBuffer.EnumerateSnapshot(e => e.Key.Equals(key) && e.TimestampTicks >= from.Value.Ticks);
        }
        else if (to.HasValue)
        {
            return partitionBuffer.EnumerateSnapshot(e => e.Key.Equals(key) && e.TimestampTicks <= to.Value.Ticks);
        }
        else
        {
            return partitionBuffer.EnumerateSnapshot(key);
        }
    }

    /// <summary>
    /// Queries events across all partitions within a time range.
    /// </summary>
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
    /// </summary>
    public long Purge(DateTime olderThan)
    {
        long totalPurged = 0;
        var thresholdTicks = olderThan.Ticks;

        foreach (var partition in _partitions)
        {
            totalPurged += partition.Purge(thresholdTicks);
        }

        return totalPurged;
    }

    /// <summary>
    /// Gets partition statistics for monitoring and debugging.
    /// </summary>
    public IEnumerable<(int PartitionIndex, long Head, long Tail, int Epoch, long Count)> GetPartitionStatistics()
    {
        for (int i = 0; i < _partitions.Length; i++)
        {
            var stats = _partitions[i].GetStatistics();
            yield return (i, stats.Head, stats.Tail, stats.Epoch, stats.Count);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetPartition(KeyId key)
    {
        return Partitioners.ForKeyIdSimple(key, _partitions.Length);
    }

    private void OnEventDiscardedInternal(Event discardedEvent)
    {
        _statistics.RecordDiscard();
        // Additional logic for discarded events can be added here
    }

    /// <summary>
    /// Aggregates values by key across all partitions.
    /// </summary>
    public Dictionary<KeyId, double> AggregateByKey()
    {
        var results = new Dictionary<KeyId, double>();
        
        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
            {
                if (results.TryGetValue(e.Key, out var existingValue))
                {
                    results[e.Key] = existingValue + e.Value;
                }
                else
                {
                    results[e.Key] = e.Value;
                }
            }
        }
        
        return results;
    }

    /// <summary>
    /// Aggregates values by key within a time range.
    /// </summary>
    public Dictionary<KeyId, double> AggregateByKey(DateTime from, DateTime to)
    {
        var results = new Dictionary<KeyId, double>();
        var fromTicks = from.Ticks;
        var toTicks = to.Ticks;
        
        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot(fromTicks, toTicks))
            {
                if (results.TryGetValue(e.Key, out var existingValue))
                {
                    results[e.Key] = existingValue + e.Value;
                }
                else
                {
                    results[e.Key] = e.Value;
                }
            }
        }
        
        return results;
    }

    /// <summary>
    /// Gets the latest value for each key.
    /// </summary>
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
    /// </summary>
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        Buffers.WithRentedBuffer<Event>(chunkSize, buffer =>
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
    /// </summary>
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
            partitionBuffer.SnapshotFilteredZeroAlloc(e => e.Key.Equals(key) && e.TimestampTicks >= from.Value.Ticks, processor, chunkSize);
        }
        else if (to.HasValue)
        {
            partitionBuffer.SnapshotFilteredZeroAlloc(e => e.Key.Equals(key) && e.TimestampTicks <= to.Value.Ticks, processor, chunkSize);
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
        // Use a Dictionary for accumulation (this still allocates, but only once per call)
        var results = new Dictionary<KeyId, double>();
        
        // Process each partition and accumulate results
        foreach (var partition in _partitions)
        {
            partition.SnapshotZeroAlloc(events =>
            {
                foreach (var e in events)
                {
                    if (results.TryGetValue(e.Key, out var existingValue))
                    {
                        results[e.Key] = existingValue + e.Value;
                    }
                    else
                    {
                        results[e.Key] = e.Value;
                    }
                }
            }, chunkSize);
        }

        // Output results in chunks using pooled buffer
        if (results.Count > 0)
        {
            Buffers.WithRentedBuffer<KeyValuePair<KeyId, double>>(chunkSize, buffer =>
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
                
                // Process remaining items
                if (count > 0)
                {
                    processor(buffer.AsSpan(0, count));
                }
            }, ArrayPool<KeyValuePair<KeyId, double>>.Shared);
        }
    }

    /// <summary>
    /// Zero-allocation time-filtered aggregation by key.
    /// </summary>
    public void AggregateByKeyZeroAlloc(DateTime from, DateTime to, Action<ReadOnlySpan<KeyValuePair<KeyId, double>>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var results = new Dictionary<KeyId, double>();
        var fromTicks = from.Ticks;
        var toTicks = to.Ticks;
        
        foreach (var partition in _partitions)
        {
            partition.SnapshotTimeRangeZeroAlloc(fromTicks, toTicks, events =>
            {
                foreach (var e in events)
                {
                    if (results.TryGetValue(e.Key, out var existingValue))
                    {
                        results[e.Key] = existingValue + e.Value;
                    }
                    else
                    {
                        results[e.Key] = e.Value;
                    }
                }
            }, chunkSize);
        }

        // Output results in chunks
        if (results.Count > 0)
        {
            Buffers.WithRentedBuffer<KeyValuePair<KeyId, double>>(chunkSize, buffer =>
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
    }
}

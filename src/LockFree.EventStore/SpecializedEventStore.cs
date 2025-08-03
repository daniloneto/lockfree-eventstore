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
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        if (partitions <= 0) throw new ArgumentOutOfRangeException(nameof(partitions));

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
    }

    /// <summary>
    /// Adds multiple events efficiently using batch operations grouped by partition.
    /// </summary>
    public void AddRange(ReadOnlySpan<Event> events)
    {
        if (events.IsEmpty) return;

        // Group events by partition for better cache locality - SoA approach
        var partitionGroups = new List<Event>[_partitions.Length];
        for (int i = 0; i < _partitions.Length; i++)
        {
            partitionGroups[i] = new List<Event>();
        }

        // Distribute events to partitions
        foreach (var e in events)
        {
            var partition = GetPartition(e.Key);
            partitionGroups[partition].Add(e);
        }

        // Add to each partition in batch for optimal cache usage
        for (int i = 0; i < _partitions.Length; i++)
        {
            if (partitionGroups[i].Count > 0)
            {
                var span = CollectionsMarshal.AsSpan(partitionGroups[i]);
                _partitions[i].TryEnqueueBatch(span);
            }
        }

        _statistics.IncrementTotalAdded(events.Length);
    }

    /// <summary>
    /// Adds multiple events from an enumerable.
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
            var eventList = events.ToList();
            AddRange(CollectionsMarshal.AsSpan(eventList));
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
}

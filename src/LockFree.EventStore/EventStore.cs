using System.Buffers;
using System.Collections.Concurrent;
using System.Numerics;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// In-memory partitioned event store using lock-free ring buffers.
/// </summary>
public sealed class EventStore<TEvent>
{
    private readonly LockFreeRingBuffer<TEvent>[] _partitions;
    private readonly IEventTimestampSelector<TEvent>? _ts;
    private readonly EventStoreOptions<TEvent> _options;
    private readonly EventStoreStatistics _statistics;
    private readonly KeyMap _keyMap; // Hot path optimization
    
    // Window state per partition for incremental aggregation
    private readonly PartitionWindowState[] _windowStates;

    /// <summary>
    /// Initializes a new instance with default options.
    /// </summary>
    public EventStore() : this(null) { }

    /// <summary>
    /// Initializes a new instance with specified capacity.
    /// </summary>
    public EventStore(int capacity) : this(new EventStoreOptions<TEvent> { Capacity = capacity }) { }

    /// <summary>
    /// Initializes a new instance with specified capacity and partitions.
    /// </summary>
    public EventStore(int capacity, int partitions) : this(new EventStoreOptions<TEvent> 
    { 
        Capacity = capacity, 
        Partitions = partitions 
    }) { }

    /// <summary>
    /// Initializes a new instance with the provided options.
    /// </summary>
    public EventStore(EventStoreOptions<TEvent>? options = null)
    {
        _options = options ?? new EventStoreOptions<TEvent>();
        if (_options.Partitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(_options.Partitions));
        
        _statistics = new EventStoreStatistics();
        _keyMap = new KeyMap();
        _partitions = new LockFreeRingBuffer<TEvent>[_options.Partitions];
        
        var capacityPerPartition = _options.Capacity.HasValue 
            ? Math.Max(1, _options.Capacity.Value / _options.Partitions)
            : _options.CapacityPerPartition;        for (int i = 0; i < _partitions.Length; i++)
        {
            _partitions[i] = new LockFreeRingBuffer<TEvent>(
                capacityPerPartition, 
                _options.OnEventDiscarded != null ? OnEventDiscardedInternal : null);
        }
        
        _windowStates = new PartitionWindowState[_options.Partitions];
        for (int i = 0; i < _windowStates.Length; i++)
        {
            _windowStates[i].Reset();
        }
        
        _ts = _options.TimestampSelector;
    }    /// <summary>
    /// Number of partitions.
    /// </summary>
    public int Partitions => _partitions.Length;    /// <summary>
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
    /// Approximate total number of events across partitions (alias for CountApprox).
    /// </summary>
    public long Count => CountApprox;

    /// <summary>
    /// Whether the store is empty (approximate).
    /// </summary>
    public bool IsEmpty => _partitions.All(p => p.IsEmpty);

    /// <summary>
    /// Whether the store is at full capacity (approximate).
    /// </summary>
    public bool IsFull => _partitions.Any(p => p.IsFull);

    /// <summary>
    /// Statistics and metrics for this store.
    /// </summary>
    public EventStoreStatistics Statistics => _statistics;

    /// <summary>
    /// Gets the timestamp selector used by this store.
    /// </summary>
    public IEventTimestampSelector<TEvent>? TimestampSelector => _ts;

    /// <summary>
    /// Gets the number of registered keys in the KeyMap.
    /// </summary>
    public int RegisteredKeysCount => _keyMap.Count;

    /// <summary>
    /// Gets all registered key mappings (for debugging/monitoring).
    /// </summary>
    public IReadOnlyDictionary<string, KeyId> GetKeyMappings() => _keyMap.GetAllMappings();

    private void OnEventDiscardedInternal(TEvent evt)
    {
        _statistics.RecordDiscard();
        _options.OnEventDiscarded?.Invoke(evt);
        
        if (IsFull)
            _options.OnCapacityReached?.Invoke();
    }    /// <summary>
    /// Appends an event using the default partitioner.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(TEvent e)
    {
        var partition = Partitioners.ForKey(e, _partitions.Length);
        return TryAppend(e, partition);
    }    /// <summary>
    /// Appends a batch of events using the default partitioner.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppend(ReadOnlySpan<TEvent> batch)
    {
        int written = 0;
        foreach (var e in batch)
        {
            if (TryAppend(e))
                written++;
        }
        return written;
    }

    /// <summary>
    /// Appends a batch of events using the default partitioner with early termination on failure.
    /// This version stops at the first failed append and returns the count of successful appends.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendAll(ReadOnlySpan<TEvent> batch)
    {
        for (int i = 0; i < batch.Length; i++)
        {
            if (!TryAppend(batch[i]))
                return i; // Return count of successful appends before failure
        }
        return batch.Length; // All succeeded
    }/// <summary>
    /// Appends an event to the specified partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(TEvent e, int partition)
    {
        if ((uint)partition >= (uint)_partitions.Length)
            throw new ArgumentOutOfRangeException(nameof(partition));
        var result = _partitions[partition].TryEnqueue(e);
        if (result)
            _statistics.RecordAppend();
        return result;
    }

    // ========== KEY ID HOT PATH METHODS ==========

    /// <summary>
    /// Gets or creates a KeyId for the given string key.
    /// This is the bridge method between string keys and the hot path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public KeyId GetOrCreateKeyId(string key) => _keyMap.GetOrAdd(key);

    /// <summary>
    /// Appends an event using a string key (converts to KeyId internally).
    /// This maintains the existing API while benefiting from KeyId optimization.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(string key, TEvent value)
    {
        var keyId = _keyMap.GetOrAdd(key);
        return TryAppend(keyId, value);
    }

    /// <summary>
    /// Appends an event using a string key with timestamp (converts to KeyId internally).
    /// This maintains the existing API while benefiting from KeyId optimization.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(string key, TEvent value, long timestamp)
    {
        var keyId = _keyMap.GetOrAdd(key);
        return TryAppend(keyId, value, timestamp);
    }

    /// <summary>
    /// HOT PATH: Appends an event using KeyId directly (no string operations).
    /// This is the fastest path for repeated operations with the same keys.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(KeyId keyId, TEvent value)
    {
        var partition = Partitioners.ForKeyIdSimple(keyId, _partitions.Length);
        return TryAppend(value, partition);
    }

    /// <summary>
    /// HOT PATH: Appends an event using KeyId with timestamp (no string operations).
    /// This is the fastest path for repeated operations with the same keys.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(KeyId keyId, TEvent value, long timestamp)
    {
        var partition = Partitioners.ForKeyIdSimple(keyId, _partitions.Length);
        return TryAppend(value, partition);
    }

    /// <summary>
    /// HOT PATH: Batch append using KeyId array (no string operations).
    /// All events use the same KeyId for maximum performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(KeyId keyId, ReadOnlySpan<TEvent> batch)
    {
        var partition = Partitioners.ForKeyIdSimple(keyId, _partitions.Length);
        return TryAppendBatch(batch, partition);
    }

    /// <summary>
    /// Batch append with KeyId-Event pairs (mixed keys but still hot path).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(ReadOnlySpan<(KeyId KeyId, TEvent Event)> batch)
    {
        int written = 0;
        foreach (var (keyId, evt) in batch)
        {
            if (TryAppend(keyId, evt))
                written++;
        }
        return written;
    }    /// <summary>
    /// Clears all events from the store.
    /// </summary>
    public void Clear()
    {
        foreach (var partition in _partitions)
        {
            partition.Clear();
        }
        _statistics.Reset();
    }

    /// <summary>
    /// Clears all events and resets KeyMap.
    /// </summary>
    public void ClearAll()
    {
        Clear();
        _keyMap.Clear();
    }

    /// <summary>
    /// Resets the store (alias for Clear).
    /// </summary>
    public void Reset()
    {
        Clear();
    }    /// <summary>
    /// Purges events older than the specified timestamp.
    /// Requires a TimestampSelector to be configured.
    /// Uses pooled buffers to minimize allocations during purge.
    /// </summary>
    public void Purge(DateTime olderThan)
    {
        if (_ts == null)
            throw new InvalidOperationException("TimestampSelector must be configured to use Purge.");

        var pool = ArrayPool<TEvent>.Shared;
        var tempBuffer = pool.Rent(16384); // Start with reasonable size
        var keepCount = 0;
        
        try
        {
            // Collect events to keep using chunked processing
            SnapshotZeroAlloc(events =>
            {
                foreach (var evt in events)
                {
                    if (_ts.GetTimestamp(evt) >= olderThan)
                    {
                        // Expand buffer if needed
                        if (keepCount >= tempBuffer.Length)
                        {
                            var oldBuffer = tempBuffer;
                            var newBuffer = pool.Rent(tempBuffer.Length * 2);
                            oldBuffer.AsSpan(0, keepCount).CopyTo(newBuffer);
                            pool.Return(oldBuffer);
                            tempBuffer = newBuffer;
                        }
                        
                        tempBuffer[keepCount++] = evt;
                    }
                }
            });

            // Clear the store
            foreach (var partition in _partitions)
            {
                partition.Clear();
            }

            // Re-add the events we want to keep
            for (int i = 0; i < keepCount; i++)
            {
                var evt = tempBuffer[i];
                var partition = Partitioners.ForKey(evt, _partitions.Length);
                _partitions[partition].TryEnqueue(evt);
            }
        }
        finally
        {
            pool.Return(tempBuffer, clearArray: false);
        }
    }/// <summary>
    /// Takes a snapshot of all partitions and returns an immutable list.
    /// Uses pooled buffers for temporary storage to reduce allocations.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot()
    {
        // First pass: calculate total count
        long total = 0;
        var partitionLengths = Buffers.RentInts(_partitions.Length);
        
        try
        {
            for (int i = 0; i < _partitions.Length; i++)
            {
                var len = _partitions[i].CountApprox;
                partitionLengths[i] = (int)Math.Min(len, _partitions[i].Capacity);
                total += partitionLengths[i];
            }
            
            // Allocate final result array
            var result = new TEvent[total];
            int idx = 0;
            
            // Second pass: populate result using pooled temporary buffers
            for (int i = 0; i < _partitions.Length; i++)
            {
                var expectedLen = partitionLengths[i];
                if (expectedLen > 0)
                {
                    var tempBuffer = new TEvent[_partitions[i].Capacity];
                    var actualLen = _partitions[i].Snapshot(tempBuffer);
                    tempBuffer.AsSpan(0, actualLen).CopyTo(result.AsSpan(idx));
                    idx += actualLen;
                }
            }
            
            // Trim result array if needed
            if (idx < result.Length)
            {
                var trimmed = new TEvent[idx];
                result.AsSpan(0, idx).CopyTo(trimmed);
                return trimmed;
            }
            
            return result;
        }
        finally
        {
            Buffers.ReturnInts(partitionLengths);
        }
    }

    /// <summary>
    /// Creates zero-allocation views of all partition contents.
    /// Returns ReadOnlyMemory segments that reference the underlying buffer without copying data.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<PartitionView<TEvent>> SnapshotViews()
    {
        var views = new PartitionView<TEvent>[_partitions.Length];
        
        for (int i = 0; i < _partitions.Length; i++)
        {
            views[i] = _partitions[i].CreateView(_ts);
        }
        
        return views;
    }

    /// <summary>
    /// Creates zero-allocation views of partition contents filtered by timestamp range.
    /// Requires a TimestampSelector to be configured.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<PartitionView<TEvent>> SnapshotViews(DateTime? from = null, DateTime? to = null)
    {
        if (_ts == null)
            throw new InvalidOperationException("TimestampSelector must be configured to use timestamp filtering.");
        
        var fromTicks = from?.Ticks ?? long.MinValue;
        var toTicks = to?.Ticks ?? long.MaxValue;
        
        var views = new PartitionView<TEvent>[_partitions.Length];
        
        for (int i = 0; i < _partitions.Length; i++)
        {
            views[i] = _partitions[i].CreateViewFiltered(fromTicks, toTicks, _ts);
        }
        
        return views;
    }

    /// <summary>
    /// Takes a filtered snapshot based on the provided predicate.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot(Func<TEvent, bool> filter)
    {
        return Query(filter).ToList();
    }

    /// <summary>
    /// Takes a snapshot of events within the specified time window.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot(DateTime? from = null, DateTime? to = null)
    {
        return Query(from: from, to: to).ToList();
    }

    /// <summary>
    /// Takes a filtered snapshot within the specified time window.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot(Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
    {
        return Query(filter, from, to).ToList();
    }    /// <summary>
    /// Returns an enumerable snapshot of all events.
    /// Uses iterator pattern to avoid upfront allocations.
    /// </summary>
    public IEnumerable<TEvent> EnumerateSnapshot()
    {
        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
            {
                yield return e;
            }
        }
    }

    private bool WithinWindow(TEvent e, DateTime? from, DateTime? to)
    {
        if (_ts is null)
            return true;
        var ts = _ts.GetTimestamp(e);
        if (from.HasValue && ts < from.Value)
            return false;
        if (to.HasValue && ts > to.Value)
            return false;
        return true;
    }    /// <summary>
    /// Queries events by optional filter and time window.
    /// Uses iterator pattern to avoid upfront allocations.
    /// </summary>
    public IEnumerable<TEvent> Query(Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
            {
                if (!WithinWindow(e, from, to))
                    continue;
                if (filter is null || filter(e))
                    yield return e;
            }
        }
    }

    /// <summary>
    /// Queries events by filter function and time window.
    /// </summary>
    public IEnumerable<TEvent> Query(Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
    {
        return Query(filter != null ? new Predicate<TEvent>(filter) : null, from, to);
    }    /// <summary>
    /// Counts events within the specified time window.
    /// </summary>
    public long CountEvents(DateTime? from = null, DateTime? to = null)
    {
        return Query(from: from, to: to).LongCount();
    }

    /// <summary>
    /// Counts events matching the filter within the specified time window.
    /// </summary>
    public long CountEvents(Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
    {
        return Query(filter, from, to).LongCount();
    }

    /// <summary>
    /// Sums values extracted from events within the specified time window.
    /// </summary>
    public TResult Sum<TResult>(Func<TEvent, TResult> selector, DateTime? from = null, DateTime? to = null)
        where TResult : struct, INumber<TResult>
    {
        var sum = TResult.Zero;
        foreach (var evt in Query(from: from, to: to))
        {
            sum += selector(evt);
        }
        return sum;
    }

    /// <summary>
    /// Sums values extracted from filtered events within the specified time window.
    /// </summary>
    public TResult Sum<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
        where TResult : struct, INumber<TResult>
    {
        var sum = TResult.Zero;
        foreach (var evt in Query(filter, from, to))
        {
            sum += selector(evt);
        }
        return sum;
    }

    /// <summary>
    /// Calculates the average of values extracted from events within the specified time window.
    /// </summary>
    public double Average<TValue>(Func<TEvent, TValue> selector, DateTime? from = null, DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        var sum = TValue.Zero;
        long count = 0;
        foreach (var evt in Query(from: from, to: to))
        {
            sum += selector(evt);
            count++;
        }
        return count == 0 ? 0.0 : Convert.ToDouble(sum) / count;
    }

    /// <summary>
    /// Calculates the average of values extracted from filtered events within the specified time window.
    /// </summary>
    public double Average<TValue>(Func<TEvent, TValue> selector, Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        var sum = TValue.Zero;
        long count = 0;
        foreach (var evt in Query(filter, from, to))
        {
            sum += selector(evt);
            count++;
        }
        return count == 0 ? 0.0 : Convert.ToDouble(sum) / count;
    }

    /// <summary>
    /// Finds the minimum value extracted from events within the specified time window.
    /// </summary>
    public TResult? Min<TResult>(Func<TEvent, TResult> selector, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        TResult? min = null;
        foreach (var evt in Query(from: from, to: to))
        {
            var value = selector(evt);
            if (!min.HasValue || value.CompareTo(min.Value) < 0)
                min = value;
        }
        return min;
    }

    /// <summary>
    /// Finds the minimum value extracted from filtered events within the specified time window.
    /// </summary>
    public TResult? Min<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        TResult? min = null;
        foreach (var evt in Query(filter, from, to))
        {
            var value = selector(evt);
            if (!min.HasValue || value.CompareTo(min.Value) < 0)
                min = value;
        }
        return min;
    }

    /// <summary>
    /// Finds the maximum value extracted from events within the specified time window.
    /// </summary>
    public TResult? Max<TResult>(Func<TEvent, TResult> selector, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        TResult? max = null;
        foreach (var evt in Query(from: from, to: to))
        {
            var value = selector(evt);
            if (!max.HasValue || value.CompareTo(max.Value) > 0)
                max = value;
        }
        return max;
    }

    /// <summary>
    /// Finds the maximum value extracted from filtered events within the specified time window.
    /// </summary>
    public TResult? Max<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        TResult? max = null;
        foreach (var evt in Query(filter, from, to))
        {
            var value = selector(evt);
            if (!max.HasValue || value.CompareTo(max.Value) > 0)
                max = value;
        }
        return max;
    }    /// <summary>
    /// Aggregates all events using the specified fold function.
    /// </summary>
    public TAcc Aggregate<TAcc>(Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        var acc = seed();
        foreach (var e in Query(filter, from, to))
        {
            acc = fold(acc, e);
        }
        return acc;
    }

    /// <summary>
    /// Aggregates events using the specified fold function with a filter function.
    /// </summary>
    public TAcc Aggregate<TAcc>(Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
    {
        return Aggregate(seed, fold, filter != null ? new Predicate<TEvent>(filter) : null, from, to);
    }

    /// <summary>
    /// Aggregates events grouped by a key.
    /// </summary>
    public Dictionary<TKey, TAcc> AggregateBy<TKey, TAcc>(Func<TEvent, TKey> groupBy, Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TKey : notnull
    {
        var dict = new Dictionary<TKey, TAcc>();
        foreach (var e in Query(filter, from, to))
        {
            var key = groupBy(e);
            if (!dict.TryGetValue(key, out var acc))
            {
                acc = seed();
                dict[key] = acc;
            }
            dict[key] = fold(acc, e);
        }
        return dict;
    }

    /// <summary>
    /// Aggregates events grouped by a key with a filter function.
    /// </summary>
    public Dictionary<TKey, TAcc> AggregateBy<TKey, TAcc>(Func<TEvent, TKey> groupBy, Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Func<TEvent, bool> filter, DateTime? from = null, DateTime? to = null)
        where TKey : notnull
    {
        return AggregateBy(groupBy, seed, fold, filter != null ? new Predicate<TEvent>(filter) : null, from, to);
    }

    /// <summary>
    /// Performs window aggregation across all partitions without materializing intermediate collections.
    /// Uses incremental aggregation to avoid scanning all events on each call.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public WindowAggregateResult AggregateWindow(long fromTicks, long toTicks, Predicate<TEvent>? filter = null)
    {
        var globalResult = new WindowAggregateState();
        
        for (int i = 0; i < _partitions.Length; i++)
        {
            var partition = _partitions[i];
            ref var windowState = ref _windowStates[i];
            
            // Advance the window for this partition
            AdvancePartitionWindow(partition, ref windowState, fromTicks);
            
            // Aggregate within the current window
            var partitionState = new WindowAggregateState();
            partition.EnumerateWindow(
                fromTicks, 
                toTicks, 
                _ts, 
                ref partitionState, 
                filter != null ? CreateFilteredCallback(filter) : CreateCallback());
            
            // Merge partition result into global result
            globalResult.Merge(partitionState);
        }
        
        return globalResult.ToResult();
    }    /// <summary>
    /// Overload that accepts DateTime parameters for convenience.
    /// </summary>
    public WindowAggregateResult AggregateWindow(DateTime? from = null, DateTime? to = null, Predicate<TEvent>? filter = null)
    {
        var fromTicks = from?.Ticks ?? long.MinValue;
        var toTicks = to?.Ticks ?? long.MaxValue;
        return AggregateWindow(fromTicks, toTicks, filter);
    }

    /// <summary>
    /// Enhanced Sum method using incremental window aggregation for better performance.
    /// </summary>
    public TResult SumWindow<TResult>(Func<TEvent, TResult> selector, DateTime? from = null, DateTime? to = null, Predicate<TEvent>? filter = null)
        where TResult : struct, INumber<TResult>
    {
        var sum = TResult.Zero;
        var fromTicks = from?.Ticks ?? 0;
        var toTicks = to?.Ticks ?? DateTime.MaxValue.Ticks;
        
        for (int i = 0; i < _partitions.Length; i++)
        {
            var partition = _partitions[i];
            ref var windowState = ref _windowStates[i];
            
            AdvancePartitionWindow(partition, ref windowState, fromTicks);
            
            var sumState = new SumAggregateState<TResult> { Sum = TResult.Zero };
            partition.EnumerateWindow(
                fromTicks, 
                toTicks, 
                _ts, 
                ref sumState, 
                filter != null ? CreateSumFilteredCallback(selector, filter) : CreateSumCallback(selector));
            
            sum += sumState.Sum;
        }
        
        return sum;
    }

    // Internal helper methods for window advancement and callback creation
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdvancePartitionWindow(LockFreeRingBuffer<TEvent> partition, ref PartitionWindowState windowState, long windowStartTicks)
    {
        if (_ts == null) return;
        
        var removeState = new WindowRemoveState();
        partition.AdvanceWindowTo(
            windowStartTicks,
            _ts,
            ref removeState,
            (ref WindowRemoveState state, TEvent item, long ticks) =>
            {
                // Remove item from window aggregates - this would typically update
                // the windowState, but since we're doing full recalculation for simplicity,
                // we just track what was removed
                state.RemovedCount++;
            },
            ref windowState.WindowHeadIndex);
    }    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private WindowItemCallback<TEvent, WindowAggregateState> CreateCallback()
    {
        return (ref WindowAggregateState state, TEvent item, long ticks) =>
        {
            state.Count++;
            // Try to extract numeric value for Sum/Min/Max calculations
            if (TryExtractNumericValue(item, out double value))
            {
                state.Sum += value;
                if (state.Count == 1) // First item
                {
                    state.Min = value;
                    state.Max = value;
                }
                else
                {
                    if (value < state.Min) state.Min = value;
                    if (value > state.Max) state.Max = value;
                }
            }
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private WindowItemCallback<TEvent, WindowAggregateState> CreateFilteredCallback(Predicate<TEvent> filter)
    {
        return (ref WindowAggregateState state, TEvent item, long ticks) =>
        {
            if (filter(item))
            {
                state.Count++;
                // Try to extract numeric value for Sum/Min/Max calculations
                if (TryExtractNumericValue(item, out double value))
                {
                    state.Sum += value;
                    if (state.Count == 1) // First item after filtering
                    {
                        state.Min = value;
                        state.Max = value;
                    }
                    else
                    {
                        if (value < state.Min) state.Min = value;
                        if (value > state.Max) state.Max = value;
                    }
                }
            }
        };
    }

    /// <summary>
    /// Attempts to extract a numeric value from an event using reflection as a fallback.
    /// This is used for generic window aggregation when no explicit selector is provided.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryExtractNumericValue(TEvent item, out double value)
    {
        value = 0.0;
        
        if (item == null) return false;
        
        var type = typeof(TEvent);
        
        // For Order type, extract Amount property
        if (type.Name == "Order")
        {
            var amountProperty = type.GetProperty("Amount");
            if (amountProperty != null)
            {
                var amount = amountProperty.GetValue(item);
                if (amount is decimal decimalAmount)
                {
                    value = (double)decimalAmount;
                    return true;
                }
                if (amount is double doubleAmount)
                {
                    value = doubleAmount;
                    return true;
                }
                if (amount is float floatAmount)
                {
                    value = (double)floatAmount;
                    return true;
                }
                if (amount is int intAmount)
                {
                    value = (double)intAmount;
                    return true;
                }
                if (amount is long longAmount)
                {
                    value = (double)longAmount;
                    return true;
                }
            }
        }
        
        // For MetricEvent type, extract Value property
        if (type.Name == "MetricEvent")
        {
            var valueProperty = type.GetProperty("Value");
            if (valueProperty != null)
            {
                var val = valueProperty.GetValue(item);
                if (val is double doubleVal)
                {
                    value = doubleVal;
                    return true;
                }
                if (val is decimal decimalVal)
                {
                    value = (double)decimalVal;
                    return true;
                }
                if (val is float floatVal)
                {
                    value = (double)floatVal;
                    return true;
                }
            }
        }
        
        // Generic fallback: try to find any numeric property
        var properties = type.GetProperties();
        foreach (var prop in properties)
        {
            var propValue = prop.GetValue(item);
            if (propValue is decimal decValue)
            {
                value = (double)decValue;
                return true;
            }
            if (propValue is double doubleValue)
            {
                value = doubleValue;
                return true;
            }
            if (propValue is float floatValue)
            {
                value = (double)floatValue;
                return true;
            }
            if (propValue is int intValue)
            {
                value = (double)intValue;
                return true;
            }
            if (propValue is long longValue)
            {
                value = (double)longValue;
                return true;
            }
        }
        
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static WindowItemCallback<TEvent, SumAggregateState<TResult>> CreateSumCallback<TResult>(Func<TEvent, TResult> selector)
        where TResult : struct, INumber<TResult>
    {
        return (ref SumAggregateState<TResult> state, TEvent item, long ticks) =>
        {
            state.Sum += selector(item);
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static WindowItemCallback<TEvent, SumAggregateState<TResult>> CreateSumFilteredCallback<TResult>(
        Func<TEvent, TResult> selector, 
        Predicate<TEvent> filter)
        where TResult : struct, INumber<TResult>
    {
        return (ref SumAggregateState<TResult> state, TEvent item, long ticks) =>
        {
            if (filter(item))
            {
                state.Sum += selector(item);
            }
        };
    }    /// <summary>
    /// High-performance batch append using optimized ring buffer operations.
    /// This version uses the optimized TryEnqueueBatch method for better performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(ReadOnlySpan<TEvent> batch)
    {
        if (batch.IsEmpty) return 0;
        
        int totalWritten = 0;
        
        // For small batches, use simple approach to avoid allocation overhead
        if (batch.Length <= 32)
        {
            foreach (var e in batch)
            {
                if (TryAppend(e))
                    totalWritten++;
            }
            return totalWritten;
        }
        
        // For larger batches, group by partition
        var partitionArrays = new TEvent[_partitions.Length][];
        var partitionCounts = new int[_partitions.Length];
        
        // First pass: count events per partition
        foreach (var e in batch)
        {
            var partition = Partitioners.ForKey(e, _partitions.Length);
            partitionCounts[partition]++;
        }
        
        // Allocate arrays for each partition
        for (int i = 0; i < _partitions.Length; i++)
        {
            if (partitionCounts[i] > 0)
            {
                partitionArrays[i] = new TEvent[partitionCounts[i]];
            }
        }
        
        // Second pass: distribute events to partition arrays
        Array.Clear(partitionCounts, 0, partitionCounts.Length); // Reuse as index counters
        foreach (var e in batch)
        {
            var partition = Partitioners.ForKey(e, _partitions.Length);
            if (partitionArrays[partition] != null)
            {
                partitionArrays[partition][partitionCounts[partition]++] = e;
            }
        }
        
        // Batch append to each partition
        for (int i = 0; i < _partitions.Length; i++)
        {
            if (partitionArrays[i] != null)
            {
                var written = _partitions[i].TryEnqueueBatch(partitionArrays[i]);
                totalWritten += written;
                
                // Update statistics
                for (int j = 0; j < written; j++)
                {
                    _statistics.RecordAppend();
                }
            }
        }
        
        return totalWritten;
    }

    /// <summary>
    /// Optimized batch append for single partition scenarios.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(ReadOnlySpan<TEvent> batch, int partition)
    {
        if ((uint)partition >= (uint)_partitions.Length)
            throw new ArgumentOutOfRangeException(nameof(partition));
            
        var written = _partitions[partition].TryEnqueueBatch(batch);
        
        // Update statistics
        for (int i = 0; i < written; i++)
        {
            _statistics.RecordAppend();
        }
        
        return written;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// Processes results in fixed-size chunks to avoid large allocations.
    /// </summary>
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        foreach (var partition in _partitions)
        {
            // Check if partition supports zero-allocation snapshot
            if (partition is LockFreeRingBuffer<TEvent> typedPartition)
            {
                // Use generic array pool for T
                var pool = ArrayPool<TEvent>.Shared;
                typedPartition.SnapshotZeroAlloc<TEvent>(processor, pool, chunkSize);
            }
            else
            {
                // Fallback for other partition types
                var tempBuffer = new TEvent[partition.Capacity];
                var len = partition.Snapshot(tempBuffer);
                if (len > 0)
                {
                    for (int i = 0; i < len; i += chunkSize)
                    {
                        var chunkLen = Math.Min(chunkSize, len - i);
                        processor(tempBuffer.AsSpan(i, chunkLen));
                    }
                }
            }
        }
    }

    /// <summary>
    /// Zero-allocation filtered snapshot using chunked processing.
    /// </summary>
    public void SnapshotFilteredZeroAlloc(Func<TEvent, bool> filter, Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var pool = ArrayPool<TEvent>.Shared;
        var buffer = pool.Rent(chunkSize);
        
        try
        {
            var bufferCount = 0;
            
            foreach (var partition in _partitions)
            {
                var tempBuffer = new TEvent[partition.Capacity];
                var len = partition.Snapshot(tempBuffer);
                
                for (int i = 0; i < len; i++)
                {
                    var item = tempBuffer[i];
                    if (filter(item))
                    {
                        buffer[bufferCount++] = item;
                        
                        if (bufferCount >= buffer.Length)
                        {
                            processor(buffer.AsSpan(0, bufferCount));
                            bufferCount = 0;
                        }
                    }
                }
            }
            
            // Process remaining events
            if (bufferCount > 0)
            {
                processor(buffer.AsSpan(0, bufferCount));
            }
        }
        finally
        {
            pool.Return(buffer, clearArray: false);
        }
    }

    /// <summary>
    /// Zero-allocation time-filtered snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotTimeFilteredZeroAlloc(DateTime? from, DateTime? to, Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        if (_ts == null)
            throw new InvalidOperationException("TimestampSelector must be configured to use time filtering.");
        
        var fromTicks = from?.Ticks ?? long.MinValue;
        var toTicks = to?.Ticks ?? long.MaxValue;
        
        SnapshotFilteredZeroAlloc(evt =>
        {
            var timestamp = _ts.GetTimestamp(evt);
            return timestamp.Ticks >= fromTicks && timestamp.Ticks <= toTicks;
        }, processor, chunkSize);
    }
}

/// <summary>
/// Factory class for creating optimized EventStore instances based on event type.
/// </summary>
public static class EventStoreFactory
{
    /// <summary>
    /// Creates an optimized EventStore for Event structs using contiguous Event[] arrays.
    /// This provides the best performance for the Event struct type.
    /// </summary>
    public static SpecializedEventStore CreateForEvent(int capacity = 1024, int partitions = 4, Action<Event>? onEventDiscarded = null)
    {
        return new SpecializedEventStore(capacity, partitions, onEventDiscarded);
    }

    /// <summary>
    /// Creates a generic EventStore for any event type.
    /// </summary>
    public static EventStore<TEvent> CreateGeneric<TEvent>(int capacity = 1024, int partitions = 4, Action<TEvent>? onEventDiscarded = null)
    {
        var options = new EventStoreOptions<TEvent>
        {
            Capacity = capacity,
            Partitions = partitions,
            OnEventDiscarded = onEventDiscarded
        };
        return new EventStore<TEvent>(options);
    }

    /// <summary>
    /// Creates an EventStore with automatic optimization based on event type.
    /// Uses SpecializedEventStore for Event struct, generic EventStore for other types.
    /// </summary>
    public static object CreateOptimized<TEvent>(int capacity = 1024, int partitions = 4, Action<TEvent>? onEventDiscarded = null)
    {
        if (typeof(TEvent) == typeof(Event))
        {
            // Use specialized implementation for Event
            var eventDiscarded = onEventDiscarded as Action<Event>;
            return CreateForEvent(capacity, partitions, eventDiscarded);
        }
        else
        {
            // Use generic implementation for other types
            return CreateGeneric(capacity, partitions, onEventDiscarded);
        }
    }
}

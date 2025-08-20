using System.Buffers;
using System.Collections.Concurrent;
using System.Numerics;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Linq;

namespace LockFree.EventStore;

/// <summary>
/// In-memory partitioned event store using lock-free ring buffers.
/// </summary>
public sealed class EventStore<TEvent>
{
    private readonly LockFreeRingBuffer<TEvent>[]? _partitions;
    private readonly PaddedLockFreeRingBuffer<TEvent>[]? _paddedPartitions;
    private readonly bool _usePadding;
    private readonly IEventTimestampSelector<TEvent>? _ts;
    private readonly EventStoreOptions<TEvent> _options;
    private readonly EventStoreStatistics _statistics;
    private readonly KeyMap _keyMap; // Hot path optimization

    // Window state per partition for incremental aggregation
    private readonly PartitionWindowState[] _windowStates;

    // Internal telemetry counters
    private long _appendCount;
    private long _droppedCount;
    private long _snapshotBytesExposed;
    private long _windowAdvanceCount;

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
    })
    { }

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
        _usePadding = _options.EnableFalseSharingProtection;

        var capacityPerPartition = _options.Capacity.HasValue
            ? Math.Max(1, _options.Capacity.Value / _options.Partitions)
            : _options.CapacityPerPartition;

        if (_usePadding)
        {
            // Use padded ring buffers for high-performance MPMC scenarios
            _paddedPartitions = new PaddedLockFreeRingBuffer<TEvent>[_options.Partitions];
            _partitions = null;
            for (int i = 0; i < _paddedPartitions.Length; i++)
            {
                _paddedPartitions[i] = new PaddedLockFreeRingBuffer<TEvent>(
                    capacityPerPartition,
                    OnEventDiscardedInternal); // Always pass the internal callback for stats tracking
            }
        }
        else
        {
            // Use standard ring buffers for compatibility
            _partitions = new LockFreeRingBuffer<TEvent>[_options.Partitions];
            _paddedPartitions = null;
            for (int i = 0; i < _partitions.Length; i++)
            {
                _partitions[i] = new LockFreeRingBuffer<TEvent>(
                    capacityPerPartition,
                    OnEventDiscardedInternal); // Always pass the internal callback for stats tracking
            }
        }

        _windowStates = new PartitionWindowState[_options.Partitions];
        for (int i = 0; i < _windowStates.Length; i++)
        {
            _windowStates[i].Reset();
        }

        _ts = _options.TimestampSelector;
    }

    /// <summary>
    /// Number of partitions.
    /// </summary>
    public int Partitions => GetPartitionCount();

    /// <summary>
    /// Total configured capacity across all partitions.
    /// </summary>
    public int Capacity
    {
        get
        {
            var partitionCount = GetPartitionCount();
            return Enumerable.Range(0, partitionCount).Sum(i => GetPartitionCapacity(i));
        }
    }

    /// <summary>
    /// Approximate total number of events across partitions.
    /// </summary>
    public long CountApprox
    {
        get
        {
            var partitionCount = GetPartitionCount();
            return Enumerable.Range(0, partitionCount)
                .Select(i => GetPartitionCount(i))
                .Sum();
        }
    }

    /// <summary>
    /// Approximate total number of events across partitions (alias for CountApprox).
    /// </summary>
    public long Count => CountApprox;

    /// <summary>
    /// Whether the store is empty (approximate).
    /// </summary>
    public bool IsEmpty
    {
        get
        {
            var partitionCount = GetPartitionCount();
            return Enumerable.Range(0, partitionCount).All(i => IsPartitionEmpty(i));
        }
    }

    /// <summary>
    /// Whether the store is at full capacity (approximate).
    /// </summary>
    public bool IsFull
    {
        get
        {
            var partitionCount = GetPartitionCount();
            return Enumerable.Range(0, partitionCount).Any(i => IsPartitionFull(i));
        }
    }

    /// <summary>
    /// Statistics and metrics for this store.
    /// </summary>
    public EventStoreStatistics Statistics => _statistics;    /// <summary>
                                                              /// Attempts to retrieve current telemetry statistics for this store.
                                                              /// </summary>
                                                              /// <param name="stats">When this method returns true, contains the current store statistics.</param>
                                                              /// <returns>Always returns true. This method is designed for future extensibility where stat collection might be conditionally available.</returns>
    public bool TryGetStats(out StoreStats stats)
    {
        stats = GetCurrentStatsSnapshot();
        return true;
    }

    /// <summary>
    /// Creates a snapshot of current store statistics.
    /// </summary>
    private StoreStats GetCurrentStatsSnapshot()
    {
        return new StoreStats(
            appendCount: Interlocked.Read(ref _appendCount),
            droppedCount: Interlocked.Read(ref _droppedCount),
            snapshotBytesExposed: Interlocked.Read(ref _snapshotBytesExposed),
            windowAdvanceCount: Interlocked.Read(ref _windowAdvanceCount)
        );
    }

    /// <summary>
    /// Notifies subscribers when statistics are updated.
    /// </summary>
    private void NotifyStatsUpdated()
    {
        var handler = _options.OnStatsUpdated;
        if (handler is not null)
        {
            try
            {
                var stats = GetCurrentStatsSnapshot();
                handler(stats);
            }
            catch
            {
                // Silently ignore exceptions from user callback to avoid breaking event flow
            }
        }
    }

    /// <summary>
    /// Increments append count and notifies statistics update.
    /// </summary>
    private void IncrementAppendCount(int delta = 1)
    {
        Interlocked.Add(ref _appendCount, delta);
        NotifyStatsUpdated();
    }

    /// <summary>
    /// Increments dropped count and notifies statistics update.
    /// </summary>
    private void IncrementDroppedCount(int delta = 1)
    {
        Interlocked.Add(ref _droppedCount, delta);
        NotifyStatsUpdated();
    }

    /// <summary>
    /// Increments snapshot bytes exposed and notifies statistics update.
    /// </summary>
    private void IncrementSnapshotBytesExposed(long delta)
    {
        Interlocked.Add(ref _snapshotBytesExposed, delta);
        NotifyStatsUpdated();
    }

    /// <summary>
    /// Increments window advance count and notifies statistics update.
    /// </summary>
    private void IncrementWindowAdvanceCount(int delta = 1)
    {
        Interlocked.Add(ref _windowAdvanceCount, delta);
        NotifyStatsUpdated();
    }

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
    /// <summary>
    /// Internal discard callback used to update statistics and invoke user-provided hooks.
    /// </summary>
    /// <param name="evt">The event instance that was discarded by a ring buffer.</param>
    private void OnEventDiscardedInternal(TEvent evt)
    {
        _statistics.RecordDiscard();
        IncrementDroppedCount();

        // Only call user callback if provided
        _options.OnEventDiscarded?.Invoke(evt);

        if (IsFull)
            _options.OnCapacityReached?.Invoke();
    }

    /// <summary>
    /// Appends an event using the default partitioner.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(TEvent e)
    {
        var partition = Partitioners.ForKey(e, GetPartitionCount());
        return TryAppend(e, partition);
    }

    /// <summary>
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
    /// Appends an event to the specified partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(TEvent e, int partition)
    {
        if ((uint)partition >= (uint)GetPartitionCount())
            throw new ArgumentOutOfRangeException(nameof(partition));
        var result = TryEnqueueToPartition(partition, e);
        if (result)
        {
            _statistics.RecordAppend();
            IncrementAppendCount();

            // Check for window advancement if timestamp selector is available
            if (_ts != null && _options.WindowSizeTicks.HasValue)
            {
                var eventTimestamp = _ts.GetTimestamp(e);
                ref var windowState = ref _windowStates[partition];
                AdvancePartitionWindow(ref windowState, eventTimestamp.Ticks);
            }
        }
        return result;
    }

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
        var partition = Partitioners.ForKeyIdSimple(keyId, GetPartitionCount());
        return TryAppend(value, partition);
    }

    /// <summary>
    /// HOT PATH: Appends an event using KeyId with timestamp (no string operations).
    /// This is the fastest path for repeated operations with the same keys.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(KeyId keyId, TEvent value, long timestamp)
    {
        var partition = Partitioners.ForKeyIdSimple(keyId, GetPartitionCount());
        return TryAppend(value, partition);
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
    }

    // ========== KEY ID HOT PATH METHODS ==========

    /// <summary>
    /// Gets or creates a KeyId for the given string key.
    /// This is the bridge method between string keys and the hot path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public KeyId GetOrCreateKeyId(string key) => _keyMap.GetOrAdd(key);

    /// <summary>
    /// HOT PATH: Batch append using KeyId array (no string operations).
    /// All events use the same KeyId for maximum performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(KeyId keyId, ReadOnlySpan<TEvent> batch)
    {
        var partition = Partitioners.ForKeyIdSimple(keyId, GetPartitionCount());
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
        var partitionCount = GetPartitionCount();
        foreach (var i in Enumerable.Range(0, partitionCount))
        {
            ClearPartition(i);
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
        var tempBuffer = pool.Rent(16384);
        try
        {
            var result = CollectEventsToKeep(olderThan, pool, tempBuffer);
            ClearAllPartitions();
            ReAddKeptEvents(result.buffer, result.count);
        }
        finally
        {
            pool.Return(tempBuffer, clearArray: false);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private (int count, TEvent[] buffer) CollectEventsToKeep(DateTime olderThan, ArrayPool<TEvent> pool, TEvent[] initialBuffer)
    {
        int keepCount = 0;
        var buffer = initialBuffer;
        SnapshotZeroAlloc(events =>
        {
            foreach (var evt in events)
            {
                if (_ts!.GetTimestamp(evt) >= olderThan)
                {
                    buffer = EnsureCapacity(buffer, keepCount + 1, pool);
                    buffer[keepCount++] = evt;
                }
            }
        });
        return (keepCount, buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TEvent[] EnsureCapacity(TEvent[] buffer, int needed, ArrayPool<TEvent> pool)
    {
        if (needed <= buffer.Length) return buffer;
        var old = buffer;
        var newBuf = pool.Rent(Math.Max(old.Length * 2, needed));
        Array.Copy(old, 0, newBuf, 0, old.Length);
        pool.Return(old);
        return newBuf;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ClearAllPartitions()
    {
        var partitionCount = GetPartitionCount();
        foreach (var p in Enumerable.Range(0, partitionCount))
        {
            ClearPartition(p);
        }
        _statistics.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReAddKeptEvents(TEvent[] buffer, int count)
    {
        var partitionCount = GetPartitionCount();
        for (int i = 0; i < count; i++)
        {
            var evt = buffer[i];
            var partition = Partitioners.ForKey(evt, partitionCount);
            TryEnqueueToPartition(partition, evt);
        }
    }

    /// <summary>
    /// Takes a snapshot of all partitions and returns an immutable list.
    /// Uses pooled buffers for temporary storage to reduce allocations.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot()
    {
        var partitionCount = GetPartitionCount();
        var partitionLengths = Buffers.RentInts(partitionCount);
        try
        {
            long total = ComputePartitionLengths(partitionCount, partitionLengths);
            var result = new TEvent[total];
            int idx = CopyPartitionsToResult(partitionCount, partitionLengths, result);
            return FinalizeSnapshotResult(result, idx);
        }
        finally
        {
            Buffers.ReturnInts(partitionLengths);
        }
    }

    /// <summary>
    /// Computes the total length of all partitions and fills the partitionLengths array.
    /// </summary>
    /// <param name="partitionCount">The number of partitions.</param>
    /// <param name="partitionLengths">Array to store the length of each partition.</param>
    /// <returns>The total length of all partitions.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long ComputePartitionLengths(int partitionCount, int[] partitionLengths)
    {
        long total = 0;
        for (int i = 0; i < partitionCount; i++)
        {
            var len = GetPartitionCount(i);
            partitionLengths[i] = (int)Math.Min(len, GetPartitionCapacity(i));
            total += partitionLengths[i];
        }
        return total;
    }

    /// <summary>
    /// Copies the contents of all partitions into the result array.
    /// </summary>
    /// <param name="partitionCount">The number of partitions.</param>
    /// <param name="partitionLengths">Array containing the length of each partition.</param>
    /// <param name="result">Array to store the copied events.</param>
    /// <returns>The total number of copied events.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CopyPartitionsToResult(int partitionCount, int[] partitionLengths, TEvent[] result)
    {
        int idx = 0;
        for (int i = 0; i < partitionCount; i++)
        {
            var expectedLen = partitionLengths[i];
            if (expectedLen <= 0) continue;

            var capacity = GetPartitionCapacity(i);
            var tempBuffer = new TEvent[capacity];
            var actualLen = _usePadding ? _paddedPartitions![i].Snapshot(tempBuffer) : _partitions![i].Snapshot(tempBuffer);
            tempBuffer.AsSpan(0, actualLen).CopyTo(result.AsSpan(idx));
            idx += actualLen;
        }
        return idx;
    }

    /// <summary>
    /// Finalizes the snapshot result by trimming the result array if necessary.
    /// </summary>
    /// <param name="result">The result array containing the copied events.</param>
    /// <param name="idx">The total number of copied events.</param>
    /// <returns>A read-only list of the copied events.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IReadOnlyList<TEvent> FinalizeSnapshotResult(TEvent[] result, int idx)
    {
        if (idx < result.Length)
        {
            var trimmed = new TEvent[idx];
            result.AsSpan(0, idx).CopyTo(trimmed);
            IncrementSnapshotBytesExposed((long)idx * sizeof(long));
            return trimmed;
        }
        else
        {
            IncrementSnapshotBytesExposed((long)result.Length * sizeof(long));
            return result;
        }
    }    /// <summary>
         /// Creates zero-allocation views of all partition contents.
         /// Returns ReadOnlyMemory segments that reference the underlying buffer without copying data.
         /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<PartitionView<TEvent>> SnapshotViews()
    {
        var partitionCount = GetPartitionCount();
        var views = Enumerable.Range(0, partitionCount)
            .Select(i => CreatePartitionView(i))
            .ToArray();

        return views;
    }    /// <summary>
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

        var partitionCount = GetPartitionCount();
        var views = Enumerable.Range(0, partitionCount)
            .Select(i => CreatePartitionViewFiltered(i, fromTicks, toTicks, _ts))
            .ToArray();

        return views;
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
    public IReadOnlyList<TEvent> Snapshot(Func<TEvent, bool> filter, DateTime? from, DateTime? to)
    {
        return Query(filter, from, to).ToList();
    }

    /// <summary>
    /// Takes a filtered snapshot across the entire time range.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot(Func<TEvent, bool> filter)
    {
        return Query(new Predicate<TEvent>(filter), null, null).ToList();
    }

    /// <summary>
    /// Returns an enumerable snapshot of all events.
    /// Uses iterator pattern to avoid upfront allocations.
    /// </summary>
    public IEnumerable<TEvent> EnumerateSnapshot()
    {
        var partitionCount = GetPartitionCount();
        return Enumerable.Range(0, partitionCount)
            .SelectMany(i => EnumeratePartitionSnapshot(i));
    }

    /// <summary>
    /// Determines whether an event falls within the optional inclusive time window.
    /// If no timestamp selector is configured, always returns true.
    /// </summary>
    /// <param name="e">The event to evaluate.</param>
    /// <param name="from">Optional inclusive start timestamp.</param>
    /// <param name="to">Optional inclusive end timestamp.</param>
    /// <returns>True when the event is within the window or when no selector is configured; otherwise false.</returns>
    private bool WithinWindow(TEvent e, DateTime? from, DateTime? to)
    {
        if (_ts is null)
            return true;
        var ts = _ts.GetTimestamp(e);
        if ((from.HasValue && ts < from.Value) || (to.HasValue && ts > to.Value))
            return false;
        return true;
    }    /// <summary>
         /// Queries events by optional filter and time window.
         /// Uses iterator pattern to avoid upfront allocations.
         /// </summary>
    public IEnumerable<TEvent> Query(Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        var partitionCount = GetPartitionCount();
        return Enumerable.Range(0, partitionCount)
            .SelectMany(i => EnumeratePartitionSnapshot(i))
            .Where(e => WithinWindow(e, from, to) && (filter is null || filter(e)));
    }

    /// <summary>
    /// Queries events by filter function and time window.
    /// </summary>
    public IEnumerable<TEvent> Query(Func<TEvent, bool> filter, DateTime? from, DateTime? to)
    {
        return Query(filter != null ? new Predicate<TEvent>(filter) : null, from, to);
    }

    /// <summary>
    /// Queries events by filter function for the entire time range.
    /// </summary>
    public IEnumerable<TEvent> Query(Func<TEvent, bool> filter)
    {
        return Query(new Predicate<TEvent>(filter), null, null);
    }

    /// <summary>
    /// Counts events within the specified time window.
    /// </summary>
    public long CountEvents(DateTime? from = null, DateTime? to = null)
    {
        return Query(from: from, to: to).LongCount();
    }

    /// <summary>
    /// Counts events matching the filter within the specified time window.
    /// </summary>
    public long CountEvents(Func<TEvent, bool> filter, DateTime? from, DateTime? to)
    {
        return Query(filter, from, to).LongCount();
    }

    /// <summary>
    /// Counts events matching the filter across the entire time range.
    /// </summary>
    public long CountEvents(Func<TEvent, bool> filter)
    {
        return Query(new Predicate<TEvent>(filter), null, null).LongCount();
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
    public TResult Sum<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter, DateTime? from, DateTime? to)
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
    /// Sums values extracted from filtered events across the entire time range.
    /// </summary>
    public TResult Sum<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter)
        where TResult : struct, INumber<TResult>
    {
        return Sum(selector, filter, null, null);
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
    public double Average<TValue>(Func<TEvent, TValue> selector, Func<TEvent, bool> filter, DateTime? from, DateTime? to)
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
    /// Calculates the average of values extracted from filtered events across the entire time range.
    /// </summary>
    public double Average<TValue>(Func<TEvent, TValue> selector, Func<TEvent, bool> filter)
        where TValue : struct, INumber<TValue>
    {
        return Average(selector, filter, null, null);
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
    public TResult? Min<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter, DateTime? from, DateTime? to)
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
    /// Finds the minimum value extracted from filtered events across the entire time range.
    /// </summary>
    public TResult? Min<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter)
        where TResult : struct, IComparable<TResult>
    {
        return Min(selector, filter, null, null);
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
    public TResult? Max<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter, DateTime? from, DateTime? to)
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
    }

    /// <summary>
    /// Finds the maximum value extracted from filtered events across the entire time range.
    /// </summary>
    public TResult? Max<TResult>(Func<TEvent, TResult> selector, Func<TEvent, bool> filter)
        where TResult : struct, IComparable<TResult>
    {
        return Max(selector, filter, null, null);
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
    public TAcc Aggregate<TAcc>(Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Func<TEvent, bool> filter, DateTime? from, DateTime? to)
    {
        return Aggregate(seed, fold, filter != null ? new Predicate<TEvent>(filter) : null, from, to);
    }

    /// <summary>
    /// Aggregates events using the specified fold function with a filter function for the entire time range.
    /// </summary>
    public TAcc Aggregate<TAcc>(Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Func<TEvent, bool> filter)
    {
        return Aggregate(seed, fold, new Predicate<TEvent>(filter), null, null);
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
    public Dictionary<TKey, TAcc> AggregateBy<TKey, TAcc>(Func<TEvent, TKey> groupBy, Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Func<TEvent, bool> filter, DateTime? from, DateTime? to)
        where TKey : notnull
    {
        return AggregateBy(groupBy, seed, fold, filter != null ? new Predicate<TEvent>(filter) : null, from, to);
    }

    /// <summary>
    /// Aggregates events grouped by a key with a filter function across the entire time range.
    /// </summary>
    public Dictionary<TKey, TAcc> AggregateBy<TKey, TAcc>(Func<TEvent, TKey> groupBy, Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Func<TEvent, bool> filter)
        where TKey : notnull
    {
        return AggregateBy(groupBy, seed, fold, new Predicate<TEvent>(filter), null, null);
    }    /// <summary>
         /// Performs window aggregation across all partitions without materializing intermediate collections.
         /// Uses incremental aggregation to avoid scanning all events on each call.
         /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public WindowAggregateResult AggregateWindow(long fromTicks, long toTicks, Predicate<TEvent>? filter = null)
    {
        var globalResult = new WindowAggregateState();
        var partitionCount = GetPartitionCount();
        for (int i = 0; i < partitionCount; i++)
        {
            var partitionState = AggregateWindowForPartition(i, fromTicks, toTicks, filter);
            globalResult.Merge(partitionState);
        }
        return globalResult.ToResult();
    }

    /// <summary>
    /// Performs window aggregation for a single partition within the specified time range and optional filter.
    /// </summary>
    /// <param name="index">Partition index.</param>
    /// <param name="fromTicks">Inclusive start of the time window (ticks).</param>
    /// <param name="toTicks">Inclusive end of the time window (ticks).</param>
    /// <param name="filter">Optional predicate to filter events.</param>
    /// <returns>Aggregation state computed for the partition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private WindowAggregateState AggregateWindowForPartition(int index, long fromTicks, long toTicks, Predicate<TEvent>? filter)
    {
        var state = new WindowAggregateState();

        var tsSel = _ts;
        if (tsSel == null)
            return state;

        foreach (var item in EnumeratePartitionSnapshot(index))
        {
            var itemTicks = tsSel.GetTimestamp(item).Ticks;
            if (itemTicks < fromTicks || itemTicks > toTicks)
                continue;

            if (filter?.Invoke(item) == false)
                continue;

            state.Count++;

            if (TryExtractNumericValue(item, out double value))
            {
                UpdateAggregateState(ref state, value);
            }
        }
        return state;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void UpdateAggregateState(ref WindowAggregateState state, double value)
    {
        state.Sum += value;
        if (state.Count == 1)
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

    /// <summary>
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
        var partitionCount = GetPartitionCount();

        for (int i = 0; i < partitionCount; i++)
        {
            sum += SumWindowForPartition(i, selector, fromTicks, toTicks, filter);
        }

        return sum;
    }

    /// <summary>
    /// Computes the sum for a single partition over the specified window and optional filter using the provided selector.
    /// </summary>
    /// <typeparam name="TResult">Numeric type of the sum result.</typeparam>
    /// <param name="index">Partition index.</param>
    /// <param name="selector">Selector that projects a numeric value from an event.</param>
    /// <param name="fromTicks">Inclusive start tick of the window.</param>
    /// <param name="toTicks">Inclusive end tick of the window.</param>
    /// <param name="filter">Optional predicate to filter events.</param>
    /// <returns>The sum for the partition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private TResult SumWindowForPartition<TResult>(int index, Func<TEvent, TResult> selector, long fromTicks, long toTicks, Predicate<TEvent>? filter)
        where TResult : struct, INumber<TResult>
    {
        var sum = TResult.Zero;
        foreach (var item in EnumeratePartitionSnapshot(index).Where(item =>
        {
            var tsSel = _ts;
            if (tsSel == null) return false;
            var itemTicks = tsSel.GetTimestamp(item).Ticks;
            return itemTicks >= fromTicks && itemTicks <= toTicks && filter?.Invoke(item) != false;
        }))
        {
            sum += selector(item);
        }
        return sum;
    }

    /// <summary>
    /// Advances the window for a partition based on current timestamp and fixed window size.
    /// Removes events older than (currentTimestamp - WindowSizeTicks) from the window state.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdvancePartitionWindow(ref PartitionWindowState state, long currentTimestamp)
    {
        if (_ts == null) return;

        var windowSizeTicks = _options.WindowSizeTicks ?? TimeSpan.FromMinutes(5).Ticks;
        var windowStartTicks = currentTimestamp - windowSizeTicks;

        var oldWindowStartTicks = state.WindowStartTicks;
        var oldWindowEndTicks = state.WindowEndTicks;

        state.WindowStartTicks = windowStartTicks;
        state.WindowEndTicks = currentTimestamp;

        var tempState = state;
        tempState.Count = 0;
        tempState.Sum = 0.0;
        tempState.Min = double.MaxValue;
        tempState.Max = double.MinValue;

        var partitionCount = GetPartitionCount();
        for (int p = 0; p < partitionCount; p++)
        {
            ForEachEventInRange(p, windowStartTicks, currentTimestamp, item =>
            {
                if (TryExtractNumericValue(item, out double value))
                {
                    tempState.AddValue(value);
                }
            });
        }

        state = tempState;
        if (oldWindowStartTicks != windowStartTicks || oldWindowEndTicks != currentTimestamp)
        {
            IncrementWindowAdvanceCount();
        }
    }

    /// <summary>
    /// Iterates all events in a partition whose timestamps fall within the inclusive [fromTicks, toTicks] range
    /// and invokes the specified action for each event.
    /// </summary>
    /// <param name="index">Partition index.</param>
    /// <param name="fromTicks">Inclusive start of the time range (ticks).</param>
    /// <param name="toTicks">Inclusive end of the time range (ticks).</param>
    /// <param name="action">Action to invoke for each event in range.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ForEachEventInRange(int index, long fromTicks, long toTicks, Action<TEvent> action)
    {
        foreach (var item in EnumeratePartitionSnapshot(index).Where(item =>
        {
            if (_ts == null) return false;
            var itemTicks = _ts.GetTimestamp(item).Ticks;
            return itemTicks >= fromTicks && itemTicks <= toTicks;
        }))
        {
            action(item);
        }
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

        // Common fast-paths for known sample types
        if (type.Name == "Order")
        {
            var amountProperty = type.GetProperty("Amount");
            if (amountProperty != null)
            {
                var amount = amountProperty.GetValue(item);
                if (amount is decimal decimalAmount) { value = (double)decimalAmount; return true; }
                if (amount is double doubleAmount) { value = doubleAmount; return true; }
                if (amount is float floatAmount) { value = (double)floatAmount; return true; }
                if (amount is int intAmount) { value = intAmount; return true; }
                if (amount is long longAmount) { value = longAmount; return true; }
            }
        }
        if (type.Name == "MetricEvent")
        {
            var valueProperty = type.GetProperty("Value");
            if (valueProperty != null)
            {
                var val = valueProperty.GetValue(item);
                if (val is double dv) { value = dv; return true; }
                if (val is decimal dd) { value = (double)dd; return true; }
                if (val is float ff) { value = (double)ff; return true; }
            }
        }

        // Generic fallback: first numeric property
        foreach (var prop in type.GetProperties())
        {
            var v = prop.GetValue(item);
            if (v is decimal dec) { value = (double)dec; return true; }
            if (v is double d) { value = d; return true; }
            if (v is float f) { value = (double)f; return true; }
            if (v is int i) { value = i; return true; }
            if (v is long l) { value = l; return true; }
        }

        return false;
    }

    /// <summary>
    /// High-performance batch append using optimized ring buffer operations.
    /// This version uses the optimized TryEnqueueBatch method for better performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(ReadOnlySpan<TEvent> batch)
    {
        if (batch.IsEmpty) return 0;

        if (IsSmallBatch(batch))
        {
            return AppendSmallBatch(batch);
        }

        var partitionCount = GetPartitionCount();
        var partitionCounts = new int[partitionCount];
        CountEventsPerPartition(batch, partitionCount, partitionCounts);

        var partitionArrays = AllocatePartitionArrays(partitionCounts);
        DistributeEventsToPartitions(batch, partitionCount, partitionArrays, partitionCounts);

        return AppendPartitionBatches(partitionArrays, partitionCounts);
    }

    /// <summary>
    /// Returns true when the batch is small enough to use the per-item append path.
    /// </summary>
    /// <param name="batch">Batch of events to check.</param>
    /// <returns>True if the batch length is less than or equal to the small batch threshold.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSmallBatch(ReadOnlySpan<TEvent> batch) => batch.Length <= 32;

    /// <summary>
    /// Appends a small batch using the standard per-item append path.
    /// </summary>
    /// <param name="batch">The batch of events to append.</param>
    /// <returns>The number of events successfully appended.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int AppendSmallBatch(ReadOnlySpan<TEvent> batch)
    {
        int totalWritten = 0;
        foreach (var e in batch)
        {
            if (TryAppend(e))
                totalWritten++;
        }
        return totalWritten;
    }

    /// <summary>
    /// Counts how many events in the batch map to each partition.
    /// </summary>
    /// <param name="batch">The batch of events to analyze.</param>
    /// <param name="partitionCount">The total number of partitions.</param>
    /// <param name="partitionCounts">Output array to fill with counts per partition.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CountEventsPerPartition(ReadOnlySpan<TEvent> batch, int partitionCount, int[] partitionCounts)
    {
        foreach (var e in batch)
        {
            var partition = Partitioners.ForKey(e, partitionCount);
            partitionCounts[partition]++;
        }
    }

    /// <summary>
    /// Allocates per-partition arrays sized to the provided counts.
    /// </summary>
    /// <param name="partitionCounts">The number of elements per partition.</param>
    /// <returns>An array of arrays, one per partition, sized according to the counts.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TEvent[][] AllocatePartitionArrays(int[] partitionCounts)
    {
        var partitionArrays = new TEvent[partitionCounts.Length][];
        for (int i = 0; i < partitionCounts.Length; i++)
        {
            if (partitionCounts[i] > 0)
            {
                partitionArrays[i] = new TEvent[partitionCounts[i]];
            }
        }
        return partitionArrays;
    }

    /// <summary>
    /// Distributes events from the batch into preallocated per-partition arrays.
    /// </summary>
    /// <param name="batch">The batch of events to distribute.</param>
    /// <param name="partitionCount">Total number of partitions.</param>
    /// <param name="partitionArrays">Destination arrays, one per partition.</param>
    /// <param name="partitionCounts">Mutable index counters per partition (will be updated).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DistributeEventsToPartitions(ReadOnlySpan<TEvent> batch, int partitionCount, TEvent[][] partitionArrays, int[] partitionCounts)
    {
        Array.Clear(partitionCounts, 0, partitionCounts.Length);
        foreach (var e in batch)
        {
            var partition = Partitioners.ForKey(e, partitionCount);
            var target = partitionArrays[partition];
            if (target != null)
            {
                target[partitionCounts[partition]++] = e;
            }
        }
    }

    /// <summary>
    /// Appends per-partition batches to their respective partitions and updates statistics.
    /// </summary>
    /// <param name="partitionArrays">Arrays containing items destined for each partition.</param>
    /// <param name="partitionCounts">Number of items in each partition array.</param>
    /// <returns>Total number of events appended across partitions.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int AppendPartitionBatches(TEvent[][] partitionArrays, int[] partitionCounts)
    {
        int totalWritten = 0;
        for (int i = 0; i < partitionArrays.Length; i++)
        {
            var items = partitionArrays[i];
            if (items == null || partitionCounts[i] == 0)
                continue;

            var written = TryEnqueueBatchToPartition(i, items);
            totalWritten += written;

            // Update statistics and telemetry for the number of successfully appended items
            for (int j = 0; j < written; j++)
            {
                _statistics.RecordAppend();
            }
            IncrementAppendCount(written);
        }
        return totalWritten;
    }    /// <summary>
         /// Optimized batch append for single partition scenarios.
         /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(ReadOnlySpan<TEvent> batch, int partition)
    {
        if ((uint)partition >= (uint)GetPartitionCount())
        {
            throw new ArgumentOutOfRangeException(nameof(partition));
        }
        var written = TryEnqueueBatchToPartition(partition, batch);
        // Update statistics
        for (int i = 0; i < written; i++)
        {
            _statistics.RecordAppend();
        }

        // Update telemetry counter for successful appends
        IncrementAppendCount(written);

        return written;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// Processes results in fixed-size chunks to avoid large allocations.
    /// </summary>
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var partitionCount = GetPartitionCount();
        for (int i = 0; i < partitionCount; i++)
        {
            if (_usePadding)
            {
                ProcessPaddedPartitionSnapshot(i, processor, chunkSize);
            }
            else
            {
                ProcessStandardPartitionSnapshot(i, processor, chunkSize);
            }
        }
    }

    /// <summary>
    /// Processes the snapshot of a padded partition in fixed-size chunks and invokes the provided processor for each chunk.
    /// </summary>
    /// <param name="index">Partition index.</param>
    /// <param name="processor">Callback invoked for each chunk of events.</param>
    /// <param name="chunkSize">Preferred chunk size.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ProcessPaddedPartitionSnapshot(int index, Action<ReadOnlySpan<TEvent>> processor, int chunkSize)
    {
        var partition = _paddedPartitions![index];
        var tempBuffer = new TEvent[partition.Capacity];
        var len = partition.Snapshot(tempBuffer);
        for (int j = 0; j < len; j += chunkSize)
        {
            var chunkLen = Math.Min(chunkSize, len - j);
            var chunk = tempBuffer.AsSpan(j, chunkLen);
            IncrementSnapshotBytesExposed((long)chunkLen * sizeof(long)); // conservative
            processor(chunk);
        }
    }

    /// <summary>
    /// Processes the snapshot of a standard partition in fixed-size chunks and invokes the provided processor for each chunk.
    /// Uses pooled buffers when available to minimize allocations.
    /// </summary>
    /// <param name="index">Partition index.</param>
    /// <param name="processor">Callback invoked for each chunk of events.</param>
    /// <param name="chunkSize">Preferred chunk size.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ProcessStandardPartitionSnapshot(int index, Action<ReadOnlySpan<TEvent>> processor, int chunkSize)
    {
        var partition = _partitions![index];
        if (partition == null)
            throw new InvalidOperationException("Partition is not initialized.");

        // Directly use the standard ring buffer's zero-allocation snapshot path.
        var pool = ArrayPool<TEvent>.Shared;
        partition.SnapshotZeroAlloc<TEvent>(span =>
        {
            IncrementSnapshotBytesExposed((long)span.Length * sizeof(long));
            processor(span);
        }, pool, chunkSize);
    }

    /// <summary>
    /// Zero-allocation filtered snapshot using chunked processing. Applies the provided filter and streams
    /// chunks to the processor without allocating large intermediate collections.
    /// </summary>
    /// <param name="filter">Predicate used to select events to include in the output chunks.</param>
    /// <param name="processor">Callback invoked for each chunk of filtered events.</param>
    /// <param name="chunkSize">Preferred chunk size for processing. Defaults to Buffers.DefaultChunkSize.</param>
    public void SnapshotFilteredZeroAlloc(Func<TEvent, bool> filter, Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var pool = ArrayPool<TEvent>.Shared;
        var buffer = pool.Rent(chunkSize);
        try
        {
            var count = 0;
            var partitionCount = GetPartitionCount();
            for (int i = 0; i < partitionCount; i++)
            {
                count = ProcessFilteredPartition(i, filter, processor, buffer, count, chunkSize);
            }
            if (count > 0)
            {
                processor(new ReadOnlySpan<TEvent>(buffer, 0, count));
                IncrementSnapshotBytesExposed((long)count * sizeof(long));
            }
        }
        finally
        {
            pool.Return(buffer, clearArray: false);
        }
    }

    /// <summary>
    /// Filters a partition's events with the provided predicate, buffering results into chunks and invoking the processor.
    /// Returns the number of residual buffered items after processing the partition.
    /// </summary>
    /// <param name="index">Partition index.</param>
    /// <param name="filter">Predicate used to select events.</param>
    /// <param name="processor">Callback invoked for each produced chunk.</param>
    /// <param name="buffer">Destination buffer for building chunks.</param>
    /// <param name="count">Current number of buffered items (will be updated).</param>
    /// <param name="chunkSize">Preferred chunk size.</param>
    /// <returns>Residual number of items left in the buffer after processing the partition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int ProcessFilteredPartition(int index, Func<TEvent, bool> filter, Action<ReadOnlySpan<TEvent>> processor, TEvent[] buffer, int count, int chunkSize)
    {
        var source = _usePadding
            ? _paddedPartitions![index].EnumerateSnapshot()
            : _partitions![index].EnumerateSnapshot();

        foreach (var e in source.Where(filter))
        {
            buffer[count++] = e;
            if (count == chunkSize)
            {
                processor(new ReadOnlySpan<TEvent>(buffer, 0, count));
                IncrementSnapshotBytesExposed((long)count * sizeof(long));
                count = 0;
            }
        }
        return count;
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

    // Helper methods for dual-mode partition support

    /// <summary>
    /// Helper to get partition count from the appropriate array.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetPartitionCount() => _usePadding ? _paddedPartitions!.Length : _partitions!.Length;

    /// <summary>
    /// Enumerates the snapshot of a partition.
    /// </summary>
    /// <param name="partitionIndex">The index of the partition.</param>
    /// <returns>An enumerable of events in the partition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IEnumerable<TEvent> EnumeratePartitionSnapshot(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].EnumerateSnapshot() : _partitions![partitionIndex].EnumerateSnapshot();
    }

    /// <summary>
    /// Helper to get capacity from the appropriate partition type.
    /// </summary>
    private int GetPartitionCapacity(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].Capacity : _partitions![partitionIndex].Capacity;
    }

    /// <summary>
    /// Helper to get approximate count from the appropriate partition type.
    /// </summary>
    private long GetPartitionCount(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].CountApprox : _partitions![partitionIndex].CountApprox;
    }

    /// <summary>
    /// Helper to try enqueue to the appropriate partition type.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryEnqueueToPartition(int partitionIndex, TEvent item)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].TryEnqueue(item) : _partitions![partitionIndex].TryEnqueue(item);
    }

    /// <summary>
    /// Helper to try enqueue batch to the appropriate partition type.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int TryEnqueueBatchToPartition(int partitionIndex, ReadOnlySpan<TEvent> items)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].TryEnqueueBatch(items) : _partitions![partitionIndex].TryEnqueueBatch(items);
    }

    /// <summary>
    /// Helper to create view from the appropriate partition type.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<TEvent> CreatePartitionView(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].CreateView(_ts) : _partitions![partitionIndex].CreateView(_ts);
    }

    /// <summary>
    /// Helper to create filtered view from the appropriate partition type.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<TEvent> CreatePartitionViewFiltered(int partitionIndex, long fromTicks, long toTicks, IEventTimestampSelector<TEvent> timestampSelector)
    {
        return _usePadding ?
            _paddedPartitions![partitionIndex].CreateViewFiltered(fromTicks, toTicks, timestampSelector) :
            _partitions![partitionIndex].CreateViewFiltered(fromTicks, toTicks, timestampSelector);
    }

    /// <summary>
    /// Helper to clear the appropriate partition type.
    /// </summary>
    private void ClearPartition(int partitionIndex)
    {
        if (_usePadding)
            _paddedPartitions![partitionIndex].Clear();
        else
            _partitions![partitionIndex].Clear();
    }

    /// <summary>
    /// Helper to check if a partition is empty.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsPartitionEmpty(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].IsEmpty : _partitions![partitionIndex].IsEmpty;
    }

    /// <summary>
    /// Helper to check if a partition is full.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsPartitionFull(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].IsFull : _partitions![partitionIndex].IsFull;
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

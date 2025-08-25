using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// In-memory partitioned event store using lock-free ring buffers.
/// </summary>
public sealed class EventStore<TEvent>
{
    private readonly LockFreeRingBuffer<TEvent>[]? _partitions;
    private readonly PaddedLockFreeRingBuffer<TEvent>[]? _paddedPartitions;
    private readonly bool _usePadding;
    private readonly EventStoreOptions<TEvent> _options;
    private readonly KeyMap _keyMap; // Hot path optimization

    // Window state per partition for incremental aggregation
    private readonly PartitionWindowState[] _windowStates;

    // Internal telemetry counters (padded to minimize false sharing)
    private PaddedLong _appendCount;
    private PaddedLong _droppedCount;
    private PaddedLong _snapshotBytesExposed;
    private PaddedLong _windowAdvanceCount;

    /// <summary>
    /// Initializes a new instance with default options.
    /// </summary>
    public EventStore() : this(new EventStoreOptions<TEvent>()) { }

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
    public EventStore(EventStoreOptions<TEvent>? options)
    {
        var opts = options ?? new EventStoreOptions<TEvent>();
        opts.Validate();
        _options = opts;
        if (_options.Partitions <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Partitions must be greater than zero.");
        }
        Statistics = new EventStoreStatistics();
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
            for (var i = 0; i < _paddedPartitions.Length; i++)
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
            for (var i = 0; i < _partitions.Length; i++)
            {
                _partitions[i] = new LockFreeRingBuffer<TEvent>(
                    capacityPerPartition,
                    OnEventDiscardedInternal); // Always pass the internal callback for stats tracking
            }
        }

        _windowStates = new PartitionWindowState[_options.Partitions];
        for (var i = 0; i < _windowStates.Length; i++)
        {
            _windowStates[i].Reset();
        }

        TimestampSelector = _options.TimestampSelector;
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
            var total = 0;
            for (var i = 0; i < partitionCount; i++)
            {
                total += GetPartitionCapacity(i);
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
            var partitionCount = GetPartitionCount();
            long total = 0;
            for (var i = 0; i < partitionCount; i++)
            {
                total += GetPartitionCount(i);
            }
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
    public bool IsEmpty
    {
        get
        {
            var partitionCount = GetPartitionCount();
            for (var i = 0; i < partitionCount; i++)
            {
                if (!IsPartitionEmpty(i))
                {
                    return false;
                }
            }
            return true;
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
            for (var i = 0; i < partitionCount; i++)
            {
                if (IsPartitionFull(i))
                {
                    return true;
                }
            }
            return false;
        }
    }

    /// <summary>
    /// Statistics and metrics for this store.
    /// </summary>
    public EventStoreStatistics Statistics { get; }

    /// <summary>
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
            appendCount: Interlocked.Read(ref _appendCount.Value),
            droppedCount: Interlocked.Read(ref _droppedCount.Value),
            snapshotBytesExposed: Interlocked.Read(ref _snapshotBytesExposed.Value),
            windowAdvanceCount: Interlocked.Read(ref _windowAdvanceCount.Value)
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
    /// Increments append count and (optionally sampled) notifies statistics update.
    /// Also updates public EventStoreStatistics to keep API behavior.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void IncrementAppendCount(int delta = 1)
    {
        if (delta <= 0)
        {
            return;
        }

        // Update internal counter
        var newCount = Interlocked.Add(ref _appendCount.Value, delta);

        // Update public statistics (single path for append accounting)
        if (delta == 1)
        {
            Statistics.IncrementTotalAdded();
        }
        else
        {
            Statistics.IncrementTotalAdded(delta);
        }

        // Sampled notification on appends
        var handler = _options.OnStatsUpdated;
        if (handler is null)
        {
            return;
        }

        var interval = _options.StatsUpdateInterval;
        if (interval <= 1)
        {
            NotifyStatsUpdated();
            return;
        }

        // Use bitmask when interval is power of two and delta == 1; fallback to boundary check otherwise
        if (PerformanceHelpers.IsPowerOfTwo(interval) && delta == 1)
        {
            if ((newCount & (interval - 1)) == 0)
            {
                NotifyStatsUpdated();
            }
            return;
        }

        // Generic boundary-crossing check (handles batch increments)
        var prev = newCount - delta;
        if ((prev / interval) != (newCount / interval))
        {
            NotifyStatsUpdated();
        }
    }

    /// <summary>
    /// Increments dropped count and notifies statistics update.
    /// </summary>
    private void IncrementDroppedCount(int delta = 1)
    {
        _ = Interlocked.Add(ref _droppedCount.Value, delta);
        if (_options.OnStatsUpdated is not null)
        {
            NotifyStatsUpdated();
        }
    }

    /// <summary>
    /// Increments snapshot bytes exposed and notifies statistics update.
    /// </summary>
    private void IncrementSnapshotBytesExposed(long delta)
    {
        _ = Interlocked.Add(ref _snapshotBytesExposed.Value, delta);
        if (_options.OnStatsUpdated is not null)
        {
            NotifyStatsUpdated();
        }
    }

    /// <summary>
    /// Increments window advance count and notifies statistics update.
    /// </summary>
    private void IncrementWindowAdvanceCount(int delta = 1)
    {
        _ = Interlocked.Add(ref _windowAdvanceCount.Value, delta);
        if (_options.OnStatsUpdated is not null)
        {
            NotifyStatsUpdated();
        }
    }

    /// <summary>
    /// Gets the timestamp selector used by this store.
    /// </summary>
    public IEventTimestampSelector<TEvent>? TimestampSelector { get; }

    /// <summary>
    /// Gets the number of registered keys in the KeyMap.
    /// </summary>
    public int RegisteredKeysCount => _keyMap.Count;

    /// <summary>
    /// Gets all registered key mappings (for debugging/monitoring).
    /// </summary>
    public IReadOnlyDictionary<string, KeyId> GetKeyMappings()
    {
        return _keyMap.GetAllMappings();
    }

    /// <summary>
    /// Internal discard callback used to update statistics and invoke user-provided hooks.
    /// </summary>
    /// <param name="evt">The event instance that was discarded by a ring buffer.</param>
    private void OnEventDiscardedInternal(TEvent evt)
    {
        Statistics.RecordDiscard();
        IncrementDroppedCount();

        // Only call user callback if provided
        _options.OnEventDiscarded?.Invoke(evt);

        if (IsFull)
        {
            _options.OnCapacityReached?.Invoke();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureBucketsInitialized(int partitionIndex, ref PartitionWindowState state, long currentTimestamp)
    {
        _ = partitionIndex; // suppress unused warning (kept for future extensibility)
        if (!_options.WindowSizeTicks.HasValue)
        {
            return;
        }

        if (state.Buckets == null)
        {
            var bucketCount = Math.Max(1, _options.BucketCount);
            var windowSize = _options.WindowSizeTicks!.Value;
            var bucketWidth = _options.BucketWidthTicks ?? Math.Max(1, windowSize / bucketCount);

            state.BucketWidthTicks = bucketWidth;
            state.Buckets = new AggregateBucket[bucketCount];

            // Align window start and buckets to current time
            var windowStartTicks = currentTimestamp - windowSize;
            var remainder = ((windowStartTicks % bucketWidth) + bucketWidth) % bucketWidth;
            var firstBucketStart = windowStartTicks - remainder;
            state.WindowStartTicks = windowStartTicks;
            state.WindowEndTicks = currentTimestamp;

            state.BucketHead = 0;
            for (var i = 0; i < bucketCount; i++)
            {
                var start = firstBucketStart + (i * bucketWidth);
                state.Buckets[i].Reset(start);
            }

            state.Count = 0;
            state.Sum = 0.0;
            state.Min = double.MaxValue;
            state.Max = double.MinValue;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Mod(int x, int m)
    {
        var r = x % m;
        return r < 0 ? r + m : r;
    }

    // New helper for tick alignment
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long AlignDown(long ticks, long width)
    {
        var rem = ((ticks % width) + width) % width;
        return ticks - rem;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryPrepareWindowAdvance(long currentTimestamp, ref PartitionWindowState state, out long newWindowStart)
    {
        newWindowStart = 0;
        if (TimestampSelector == null || !_options.WindowSizeTicks.HasValue)
        {
            return false;
        }

        EnsureBucketsInitialized(0, ref state, currentTimestamp); // partitionIndex is unused inside EnsureBucketsInitialized for initialization path

        var windowSizeTicks = _options.WindowSizeTicks!.Value;
        newWindowStart = currentTimestamp - windowSizeTicks;

        return !IsNoopWindowAdvance(in state, newWindowStart, currentTimestamp);
    }

    /// <summary>
    /// Advances the window for a partition based on current timestamp and fixed window size.
    /// Rolls the bucket ring forward and evicts buckets that have left the window.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdvancePartitionWindow(int partitionIndex, ref PartitionWindowState state, long currentTimestamp)
    {
        _ = partitionIndex; // suppress unused warning (kept for future extensibility)
        if (!TryPrepareWindowAdvance(currentTimestamp, ref state, out var newWindowStart))
        {
            return;
        }

        var prevStart = state.WindowStartTicks;
        var prevEnd = state.WindowEndTicks;

        // If the candidate window start moved backwards or did not progress, don't roll buckets.
        if (newWindowStart <= prevStart)
        {
            // Extend end only if timestamp moved forward
            if (currentTimestamp > prevEnd)
            {
                state.WindowEndTicks = currentTimestamp;
                IncrementWindowAdvanceCount();
            }
            return;
        }

        var buckets = state.Buckets!;
        var bucketCount = buckets.Length;
        var width = state.BucketWidthTicks;

        // Compute advances based on bucket boundary crossings, not raw ticks
        var prevAlignedStart = buckets[state.BucketHead].StartTicks; // invariant: head contains WindowStart (floor-aligned)
        var newAlignedStart = AlignDown(newWindowStart, width);
        var diffTicks = newAlignedStart - prevAlignedStart;
        if (diffTicks <= 0)
        {
            // No boundary crossed; just advance end and start
            state.WindowStartTicks = newWindowStart;
            state.WindowEndTicks = currentTimestamp > prevEnd ? currentTimestamp : prevEnd;
            IncrementWindowAdvanceCount();
            return;
        }

        var advanceBuckets = (int)(diffTicks / width);
        if (advanceBuckets >= bucketCount)
        {
            ResetAllBucketsForNewWindow(ref state, newAlignedStart);
            state.WindowStartTicks = newWindowStart;
            state.WindowEndTicks = currentTimestamp > prevEnd ? currentTimestamp : prevEnd;
            IncrementWindowAdvanceCount();
            return;
        }

        // Roll ring and reset new tail buckets aligned to newAlignedStart
        EvictAndRollBuckets(ref state, advanceBuckets, newAlignedStart);
        state.WindowStartTicks = newWindowStart;
        state.WindowEndTicks = currentTimestamp > prevEnd ? currentTimestamp : prevEnd;
        RecomputeMinMaxAcrossBuckets(ref state);

        IncrementWindowAdvanceCount();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNoopWindowAdvance(in PartitionWindowState state, long newWindowStart, long currentTimestamp)
    {
        return newWindowStart <= state.WindowStartTicks && currentTimestamp == state.WindowEndTicks;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ResetAllBucketsForNewWindow(ref PartitionWindowState state, long newWindowStart)
    {
        var buckets = state.Buckets!;
        var bucketCount = buckets.Length;
        var bucketWidth = state.BucketWidthTicks;

        // Re-seed buckets aligned to the floor of newWindowStart and realign BucketHead
        var firstAlignedStart = AlignDown(newWindowStart, bucketWidth);
        for (var i = 0; i < bucketCount; i++)
        {
            var start = firstAlignedStart + (i * bucketWidth);
            buckets[i].Reset(start);
        }
        // BucketHead points to the bucket that contains WindowStartTicks (floor-aligned)
        state.BucketHead = 0;
        state.Count = 0;
        state.Sum = 0.0;
        state.Min = double.MaxValue;
        state.Max = double.MinValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EvictAndRollBuckets(ref PartitionWindowState state, int advanceBuckets, long newAlignedStart)
    {
        var buckets = state.Buckets!;
        var bucketCount = buckets.Length;
        var bucketWidth = state.BucketWidthTicks;

        for (var step = 0; step < advanceBuckets; step++)
        {
            // Evict the head bucket leaving the window
            ref var evicted = ref buckets[state.BucketHead];
            if (evicted.Count > 0)
            {
                state.Count -= evicted.Count;
                state.Sum -= evicted.Sum;
                // Min/Max potentially invalid now; will recompute after roll
            }

            // Move head forward
            state.BucketHead = (state.BucketHead + 1) % bucketCount;

            // Compute new bucket's start at the tail position (aligned to new window start)
            var newTailStart = newAlignedStart + ((bucketCount - 1 - step) * bucketWidth);
            ref var toReset = ref buckets[Mod(state.BucketHead + (bucketCount - 1), bucketCount)];
            toReset.Reset(newTailStart);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RecomputeMinMaxAcrossBuckets(ref PartitionWindowState state)
    {
        var buckets = state.Buckets!;
        var min = double.MaxValue;
        var max = double.MinValue;
        for (var i = 0; i < buckets.Length; i++)
        {
            ref var b = ref buckets[i];
            if (b.Count == 0)
            {
                continue;
            }
            if (b.Min < min)
            {
                min = b.Min;
            }
            if (b.Max > max)
            {
                max = b.Max;
            }
        }
        state.Min = state.Count > 0 ? min : double.MaxValue;
        state.Max = state.Count > 0 ? max : double.MinValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void UpdateBucketOnAppend(ref PartitionWindowState state, long eventTicks, double value)
    {
        var buckets = state.Buckets;
        if (buckets == null)
        {
            return;
        }

        var windowStart = state.WindowStartTicks;
        var windowEnd = state.WindowEndTicks;
        if (eventTicks < windowStart || eventTicks > windowEnd)
        {
            // Outside current window; ignore for aggregate state
            return;
        }

        var bucketWidth = state.BucketWidthTicks;
        var offsetTicks = eventTicks - windowStart;
        var bucketOffset = (int)(offsetTicks / bucketWidth);
        if (bucketOffset >= buckets.Length)
        {
            bucketOffset = buckets.Length - 1; // clamp inclusive end to last bucket
        }
        var index = (state.BucketHead + bucketOffset) % buckets.Length;

        // Update bucket
        buckets[index].Add(value);

        // Update partition aggregate
        state.AddValue(value);
    }

    /// <summary>
    /// Ensures window tracking is enabled when a time-filtered window query is requested.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureWindowTrackingEnabledIfTimeFiltered(DateTime? from, DateTime? to)
    {
        if (!_options.EnableWindowTracking && (from.HasValue || to.HasValue))
        {
            throw new InvalidOperationException("Window tracking is disabled. EnableWindowTracking must be true to use window queries.");
        }
    }

    /// <summary>
    /// Core append without any window tracking. Hot-path minimal work only.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryAppendCoreFast(TEvent e, int partition)
    {
        if (!TryEnqueueToPartition(partition, e))
        {
            return false;
        }
        IncrementAppendCount();
        return true;
    }

    /// <summary>
    /// Core append maintaining window tracking state when configured.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryAppendWithWindow(TEvent e, int partition)
    {
        if (!TryEnqueueToPartition(partition, e))
        {
            return false;
        }

        IncrementAppendCount();

        // Check for window advancement if timestamp selector is available
        if (TimestampSelector != null && _options.WindowSizeTicks.HasValue)
        {
            var eventTimestamp = TimestampSelector.GetTimestamp(e);
            ref var windowState = ref _windowStates[partition];
            AdvancePartitionWindow(partition, ref windowState, eventTimestamp.Ticks);

            // Fast typed path using ValueSelector if configured
            var selector = _options.ValueSelector;
            if (selector is not null)
            {
                var value = selector(e);
                UpdateBucketOnAppend(ref windowState, eventTimestamp.Ticks, value);
            }
        }
        return true;
    }

    /// <summary>
    /// Appends an event using the default partitioner.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(TEvent e)
    {
        // Fast-path: when TEvent is the built-in Event struct, use KeyId-based partitioning with minimal work.
        if (typeof(TEvent) == typeof(Event))
        {
            var ev = Unsafe.As<TEvent, Event>(ref e);
            var partitions = GetPartitionCount();
            var partition = PerformanceHelpers.IsPowerOfTwo(partitions) ? ev.Key.Value & (partitions - 1) : ev.Key.Value % partitions;
            return TryAppend(e, partition);
        }

        var p = Partitioners.ForKey(e, GetPartitionCount());
        return TryAppend(e, p);
    }

    /// <summary>
    /// Appends a batch of events using the default partitioner.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppend(ReadOnlySpan<TEvent> batch)
    {
        var written = 0;
        foreach (var e in batch)
        {
            if (TryAppend(e))
            {
                written++;
            }
        }
        return written;
    }

    /// <summary>
    /// Appends an event to the specified partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAppend(TEvent e, int partition)
    {
        return (uint)partition >= (uint)GetPartitionCount()
            ? throw new ArgumentOutOfRangeException(nameof(partition))
            : !_options.EnableWindowTracking ? TryAppendCoreFast(e, partition) : TryAppendWithWindow(e, partition);
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
        if (!TryEnqueueToPartition(partition, value))
        {
            return false;
        }

        IncrementAppendCount();

        if (_options.EnableWindowTracking && TimestampSelector != null && _options.WindowSizeTicks.HasValue)
        {
            ref var windowState = ref _windowStates[partition];
            AdvancePartitionWindow(partition, ref windowState, timestamp);

            var selector = _options.ValueSelector;
            if (selector is not null)
            {
                var v = selector(value);
                UpdateBucketOnAppend(ref windowState, timestamp, v);
            }
        }
        return true;
    }

    /// <summary>
    /// Appends a batch of events using the default partitioner with early termination on failure.
    /// This version stops at the first failed append and returns the count of successful appends.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendAll(ReadOnlySpan<TEvent> batch)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            if (!TryAppend(batch[i]))
            {
                return i; // Return count of successful appends before failure
            }
        }
        return batch.Length; // All succeeded
    }

    // ========== KEY ID HOT PATH METHODS ==========

    /// <summary>
    /// Gets or creates a KeyId for the given string key.
    /// This is the bridge method between string keys and the hot path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public KeyId GetOrCreateKeyId(string key)
    {
        return _keyMap.GetOrAdd(key);
    }

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
        var written = 0;
        foreach (var (keyId, evt) in batch)
        {
            if (TryAppend(keyId, evt))
            {
                written++;
            }
        }
        return written;
    }

    /// <summary>
    /// High-performance batch append using optimized ring buffer operations.
    /// This version uses the optimized TryEnqueueBatch method for better performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryAppendBatch(ReadOnlySpan<TEvent> batch)
    {
        if (batch.IsEmpty)
        {
            return 0;
        }

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
        // Update telemetry counter for successful appends (and public statistics)
        IncrementAppendCount(written);

        return written;
    }

    /// <summary>
    /// Returns true when the batch is small enough to use the per-item append path.
    /// </summary>
    /// <param name="batch">Batch of events to check.</param>
    /// <returns>True if the batch length is less than or equal to the small batch threshold.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSmallBatch(ReadOnlySpan<TEvent> batch)
    {
        return batch.Length <= 32;
    }

    /// <summary>
    /// Appends a small batch using the standard per-item append path.
    /// </summary>
    /// <param name="batch">The batch of events to append.</param>
    /// <returns>The number of events successfully appended.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int AppendSmallBatch(ReadOnlySpan<TEvent> batch)
    {
        var totalWritten = 0;
        foreach (var e in batch)
        {
            if (TryAppend(e))
            {
                totalWritten++;
            }
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
    private static void CountEventsPerPartition(ReadOnlySpan<TEvent> batch, int partitionCount, int[] partitionCounts)
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
        for (var i = 0; i < partitionCounts.Length; i++)
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
    private static void DistributeEventsToPartitions(ReadOnlySpan<TEvent> batch, int partitionCount, TEvent[][] partitionArrays, int[] partitionCounts)
    {
        Array.Clear(partitionCounts, 0, partitionCounts.Length);
        foreach (var e in batch)
        {
            var partition = Partitioners.ForKey(e, partitionCount);
            var target = partitionArrays[partition];
            target?[partitionCounts[partition]++] = e;
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
        var totalWritten = 0;
        for (var i = 0; i < partitionArrays.Length; i++)
        {
            var items = partitionArrays[i];
            if (items == null || partitionCounts[i] == 0)
            {
                continue;
            }

            var written = TryEnqueueBatchToPartition(i, items);
            totalWritten += written;

            // Update telemetry for the number of successfully appended items (and public statistics)
            IncrementAppendCount(written);
        }
        return totalWritten;
    }

    /// <summary>
    /// Clears all events from the store.
    /// </summary>
    public void Clear()
    {
        var partitionCount = GetPartitionCount();
        for (var i = 0; i < partitionCount; i++)
        {
            ClearPartition(i);
        }
        Statistics.Reset();
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
    }

    /// <summary>
    /// Purges events older than the specified timestamp.
    /// Requires a TimestampSelector to be configured.
    /// Uses pooled buffers to minimize allocations during purge.
    /// </summary>
    public void Purge(DateTime olderThan)
    {
        if (TimestampSelector == null)
        {
            throw new InvalidOperationException("TimestampSelector must be configured to use Purge.");
        }

        var pool = ArrayPool<TEvent>.Shared;
        var tempBuffer = pool.Rent(16384);
        TEvent[]? keptBuffer = null;
        var clear = RuntimeHelpers.IsReferenceOrContainsReferences<TEvent>();
        try
        {
            var (count, buffer) = CollectEventsToKeep(olderThan, pool, tempBuffer);
            keptBuffer = buffer;
            ClearAllPartitions();
            ReAddKeptEvents(buffer, count);
        }
        finally
        {
            if (keptBuffer != null && !ReferenceEquals(keptBuffer, tempBuffer))
            {
                pool.Return(keptBuffer, clearArray: clear);
            }
            pool.Return(tempBuffer, clearArray: clear);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private (int count, TEvent[] buffer) CollectEventsToKeep(DateTime olderThan, ArrayPool<TEvent> pool, TEvent[] initialBuffer)
    {
        var keepCount = 0;
        var buffer = initialBuffer;
        SnapshotZeroAlloc(events =>
        {
            foreach (var evt in events)
            {
                if (TimestampSelector!.GetTimestamp(evt) >= olderThan)
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
        if (needed <= buffer.Length)
        {
            return buffer;
        }
        var old = buffer;
        var newBuf = pool.Rent(Math.Max(old.Length * 2, needed));
        Array.Copy(old, 0, newBuf, 0, old.Length);
        pool.Return(old, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TEvent>());
        return newBuf;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ClearAllPartitions()
    {
        var partitionCount = GetPartitionCount();
        for (var p = 0; p < partitionCount; p++)
        {
            ClearPartition(p);
        }
        Statistics.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReAddKeptEvents(TEvent[] buffer, int count)
    {
        var partitionCount = GetPartitionCount();
        for (var i = 0; i < count; i++)
        {
            var evt = buffer[i];
            var partition = Partitioners.ForKey(evt, partitionCount);
            _ = TryEnqueueToPartition(partition, evt);
        }
    }

    /// <summary>
    /// Takes a snapshot of all partitions and returns an immutable list.
    /// Uses zero-allocation partition views to avoid per-partition temporary buffers
    /// and allocates a single result array sized exactly to the total number of items.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot()
    {
        var partitionCount = GetPartitionCount();

        // First pass: capture stable views and compute exact total count
        var views = new PartitionView<TEvent>[partitionCount];
        long total = 0;
        for (var i = 0; i < partitionCount; i++)
        {
            views[i] = CreatePartitionView(i);
            total += views[i].Count;
        }

        if (total == 0)
        {
            // Return empty array (shared) to avoid allocation
            IncrementSnapshotBytesExposed(0);
            return [];
        }

        // Allocate final result array once with exact size based on captured views
        var result = new TEvent[total];

        // Second pass: copy segments directly from the captured views into the result
        var idx = 0;
        for (var i = 0; i < partitionCount; i++)
        {
            var view = views[i];

            var seg1 = view.Segment1.Span;
            if (!seg1.IsEmpty)
            {
                seg1.CopyTo(result.AsSpan(idx));
                idx += seg1.Length;
            }

            var seg2 = view.Segment2.Span; // wrap-around segment
            if (!seg2.IsEmpty)
            {
                seg2.CopyTo(result.AsSpan(idx));
                idx += seg2.Length;
            }
        }

        // Account for exposed bytes (conservative)
        IncrementSnapshotBytesExposed((long)result.Length * sizeof(long));
        return result;
    }

    /// <summary>
    /// Takes a filtered snapshot of all partitions and returns an immutable list containing only events matching the filter.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<TEvent> Snapshot(Func<TEvent, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        var list = new List<TEvent>();
        foreach (var view in SnapshotViews())
        {
            foreach (var e in view)
            {
                if (filter(e))
                {
                    list.Add(e);
                }
            }
        }
        IncrementSnapshotBytesExposed((long)list.Count * sizeof(long));
        return list;
    }

    /// <summary>
    /// Creates zero-allocation views of all partition contents.
    /// Returns ReadOnlyMemory segments that reference the underlying buffer without copying data.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<PartitionView<TEvent>> SnapshotViews()
    {
        var partitionCount = GetPartitionCount();
        var views = new PartitionView<TEvent>[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            views[i] = CreatePartitionView(i);
        }
        return views;
    }

    /// <summary>
    /// Creates zero-allocation views of partition contents filtered by timestamp range.
    /// Requires a TimestampSelector to be configured.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<PartitionView<TEvent>> SnapshotViews(DateTime? from, DateTime? to)
    {
        if (TimestampSelector == null)
        {
            throw new InvalidOperationException("TimestampSelector must be configured to use timestamp filtering.");
        }

        var fromTicks = from?.Ticks ?? long.MinValue;
        var toTicks = to?.Ticks ?? long.MaxValue;

        var partitionCount = GetPartitionCount();
        var views = new PartitionView<TEvent>[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            views[i] = CreatePartitionViewFiltered(i, fromTicks, toTicks, TimestampSelector);
        }
        return views;
    }

    /// <summary>
    /// Returns an enumerable snapshot of all events.
    /// Uses iterator pattern to avoid upfront allocations.
    /// </summary>
    public IEnumerable<TEvent> EnumerateSnapshot()
    {
        var partitionCount = GetPartitionCount();
        for (var i = 0; i < partitionCount; i++)
        {
            foreach (var e in EnumeratePartitionSnapshot(i))
            {
                yield return e;
            }
        }
    }

    // ===================== ZERO-ALLOC WRAPPERS (preferred APIs) =====================
    // These instance methods delegate to ZeroAllocationExtensions to improve discoverability
    // and allow callers to use zero-allocation paths without importing extension namespace.

    /// <summary>
    /// Counts events using zero-allocation processing. Optional filter and time window.
    /// </summary>
    /// <param name="filter">Optional event filter receiving the event and its timestamp (if available).</param>
    /// <param name="from">Inclusive start timestamp.</param>
    /// <param name="to">Inclusive end timestamp.</param>
    /// <returns>Total number of matching events.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long CountEventsZeroAlloc(EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.CountEventsZeroAlloc(this, filter, from, to);
    }

    /// <summary>
    /// Finds minimum value using a selector without allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TResult? MinZeroAlloc<TResult>(Func<TEvent, TResult> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.MinZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Performs window aggregation using a zero-allocation pipeline and a double selector.
    /// </summary>
    /// <param name="selector">Projects a double value from an event.</param>
    /// <param name="filter">Optional event filter receiving the event and its timestamp (if available).</param>
    /// <param name="from">Inclusive start timestamp.</param>
    /// <param name="to">Inclusive end timestamp.</param>
    /// <returns>Window aggregate with Count, Sum, Min, Max, Avg.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public WindowAggregateResult AggregateWindowZeroAlloc(Func<TEvent, double> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);

        if (TryBucketAggregateFastPath(filter, from, to, out var fastPathResult))
        {
            return fastPathResult;
        }

        var init = (Count: 0L, Sum: 0.0, Min: 0.0, Max: 0.0, Has: false);
        var state = ZeroAllocationExtensions.ProcessEventsChunked(
            this,
            init,
            (s, chunk) => AccumulateChunk(s, chunk, selector),
            filter,
            from,
            to);

        return ToWindowAggregateResult(state);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryBucketAggregateFastPath(EventFilter<TEvent>? filter, DateTime? from, DateTime? to, out WindowAggregateResult result)
    {
        result = default;
        if (filter is null && from.HasValue && to.HasValue && _options.ValueSelector is not null && TimestampSelector is not null && _options.WindowSizeTicks.HasValue)
        {
            if (CanUseBucketFastPath(from.Value, to.Value))
            {
                result = AggregateFromBuckets(from.Value, to.Value);
                return true;
            }
        }
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (long Count, double Sum, double Min, double Max, bool Has) AccumulateChunk(
        (long Count, double Sum, double Min, double Max, bool Has) s,
        ReadOnlySpan<TEvent> chunk,
        Func<TEvent, double> selector)
    {
        for (var i = 0; i < chunk.Length; i++)
        {
            var v = selector(chunk[i]);
            if (!s.Has)
            {
                s.Has = true;
                s.Min = v;
                s.Max = v;
            }
            else
            {
                if (v < s.Min)
                {
                    s.Min = v;
                }
                if (v > s.Max)
                {
                    s.Max = v;
                }
            }
            s.Count++;
            s.Sum += v;
        }
        return s;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static WindowAggregateResult ToWindowAggregateResult((long Count, double Sum, double Min, double Max, bool Has) s)
    {
        return s.Has
            ? new WindowAggregateResult(s.Count, s.Sum, s.Min, s.Max, s.Sum / s.Count)
            : new WindowAggregateResult(0, 0.0, 0.0, 0.0, 0.0);
    }

    /// <summary>
    /// Finds maximum value using a selector without allocations (double selector fast-path overload).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double? MaxZeroAlloc(Func<TEvent, double> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);

        if (filter is null && from.HasValue && to.HasValue && _options.ValueSelector is not null && TimestampSelector is not null && _options.WindowSizeTicks.HasValue)
        {
            if (CanUseBucketFastPath(from.Value, to.Value))
            {
                var r = AggregateFromBuckets(from.Value, to.Value);
                return r.Count > 0 ? r.Max : null;
            }
        }
        return ZeroAllocationExtensions.MaxZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Finds maximum value using a selector without allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TResult? MaxZeroAlloc<TResult>(Func<TEvent, TResult> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.MaxZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Sums numeric values using a double selector without allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double SumZeroAlloc(Func<TEvent, double> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);

        if (filter is null && from.HasValue && to.HasValue && _options.ValueSelector is not null && TimestampSelector is not null && _options.WindowSizeTicks.HasValue)
        {
            if (CanUseBucketFastPath(from.Value, to.Value))
            {
                var r = AggregateFromBuckets(from.Value, to.Value);
                return r.Sum;
            }
        }
        return ZeroAllocationExtensions.SumZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Sums numeric values using a generic selector without allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TValue SumZeroAlloc<TValue>(Func<TEvent, TValue> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.SumZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Computes average using a double selector without allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double AverageZeroAlloc(Func<TEvent, double> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);

        if (filter is null && from.HasValue && to.HasValue && _options.ValueSelector is not null && TimestampSelector is not null && _options.WindowSizeTicks.HasValue)
        {
            if (CanUseBucketFastPath(from.Value, to.Value))
            {
                var r = AggregateFromBuckets(from.Value, to.Value);
                return r.Count > 0 ? r.Sum / r.Count : 0.0;
            }
        }
        return ZeroAllocationExtensions.AverageZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Computes average using a generic numeric selector without allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double AverageZeroAlloc<TValue>(Func<TEvent, TValue> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.AverageZeroAlloc(this, selector, filter, from, to);
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

        foreach (var e in source)
        {
            if (!filter(e))
            {
                continue;
            }
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
    /// Helper to get partition count from the appropriate array.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetPartitionCount()
    {
        return _usePadding ? _paddedPartitions!.Length : _partitions!.Length;
    }

    /// <summary>
    /// Helper to get approximate count from the appropriate partition type.
    /// </summary>
    private long GetPartitionCount(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].CountApprox : _partitions![partitionIndex].CountApprox;
    }

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
        return _usePadding ? _paddedPartitions![partitionIndex].CreateView(TimestampSelector) : _partitions![partitionIndex].CreateView(TimestampSelector);
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
        {
            _paddedPartitions![partitionIndex].Clear();
        }
        else
        {
            _partitions![partitionIndex].Clear();
        }
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

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// Processes results in fixed-size chunks to avoid large allocations.
    /// </summary>
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var partitionCount = GetPartitionCount();
        for (var i = 0; i < partitionCount; i++)
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
        var pool = ArrayPool<TEvent>.Shared;
        // Use the padded ring buffer's zero-alloc snapshot helper that rents only chunk-sized buffers
        partition.SnapshotZeroAlloc<TEvent>(span =>
        {
            if (!span.IsEmpty)
            {
                IncrementSnapshotBytesExposed((long)span.Length * sizeof(long)); // conservative accounting
                processor(span);
            }
        }, pool, chunkSize);
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
        var partition = _partitions![index] ?? throw new InvalidOperationException("Partition is not initialized.");

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
            for (var i = 0; i < partitionCount; i++)
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
    /// Zero-allocation time-filtered snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotTimeFilteredZeroAlloc(DateTime? from, DateTime? to, Action<ReadOnlySpan<TEvent>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        if (TimestampSelector == null)
        {
            throw new InvalidOperationException("TimestampSelector must be configured to use time filtering.");
        }

        var fromTicks = from?.Ticks ?? long.MinValue;
        var toTicks = to?.Ticks ?? long.MaxValue;

        // Build filtered views per partition without allocating large buffers, then stream segments in chunks
        var partitionCount = GetPartitionCount();
        for (var i = 0; i < partitionCount; i++)
        {
            var view = CreatePartitionViewFiltered(i, fromTicks, toTicks, TimestampSelector);

            // Segment 1
            var seg1 = view.Segment1.Span;
            for (var j = 0; j < seg1.Length; j += chunkSize)
            {
                var len = Math.Min(chunkSize, seg1.Length - j);
                var chunk = seg1.Slice(j, len);
                if (!chunk.IsEmpty)
                {
                    IncrementSnapshotBytesExposed((long)chunk.Length * sizeof(long));
                    processor(chunk);
                }
            }

            // Segment 2 (wrap-around)
            var seg2 = view.Segment2.Span;
            for (var j = 0; j < seg2.Length; j += chunkSize)
            {
                var len = Math.Min(chunkSize, seg2.Length - j);
                var chunk = seg2.Slice(j, len);
                if (!chunk.IsEmpty)
                {
                    IncrementSnapshotBytesExposed((long)chunk.Length * sizeof(long));
                    processor(chunk);
                }
            }
        }
    }

    /// <summary>
    /// Computes a window aggregate using the per-partition bucket ring in O(B) where B = bucketCount.
    /// Requires WindowSize/Bucket configuration and ValueSelector set to maintain bucket stats on append.
    /// If buckets are not initialized, returns an empty aggregate.
    /// </summary>
    /// <remarks>
    /// This method does not allocate and is resilient to concurrent appends. Results are approximate under concurrency.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public WindowAggregateResult AggregateFromBuckets(DateTime from, DateTime to)
    {
        if (!_options.EnableWindowTracking)
        {
            throw new InvalidOperationException("Window tracking is disabled. EnableWindowTracking must be true to use window queries.");
        }

        var fromTicks = from.Ticks;
        var toTicks = to.Ticks;
        if (toTicks < fromTicks)
        {
            // Swap to ensure valid range
            (toTicks, fromTicks) = (fromTicks, toTicks);
        }

        var agg = new WindowAggregateState();
        var partitions = GetPartitionCount();
        for (var i = 0; i < partitions; i++)
        {
            var part = AggregatePartitionFromBuckets(ref _windowStates[i], fromTicks, toTicks);
            agg.Merge(part);
        }
        return agg.ToResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static WindowAggregateState AggregatePartitionFromBuckets(ref PartitionWindowState state, long fromTicks, long toTicks)
    {
        var result = new WindowAggregateState();

        if (!TryPrepareBucketScan(ref state, fromTicks, toTicks, out var buckets, out var width, out var clampedFrom, out var clampedTo))
        {
            return result;
        }

        var min = double.MaxValue;
        var max = double.MinValue;
        long count = 0;
        var sum = 0.0;

        var endExclusive = clampedTo;
        for (var i = 0; i < buckets.Length; i++)
        {
            ref var b = ref buckets[i];
            if (b.Count == 0)
            {
                continue;
            }

            if (BucketOverlaps(b.StartTicks, width, clampedFrom, endExclusive))
            {
                AccumulateBucket(ref count, ref sum, ref min, ref max, in b);
            }
        }

        if (count > 0)
        {
            result.Count = count;
            result.Sum = sum;
            result.Min = min;
            result.Max = max;
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryPrepareBucketScan(
        ref PartitionWindowState state,
        long fromTicks,
        long toTicks,
        out AggregateBucket[] buckets,
        out long width,
        out long clampedFrom,
        out long clampedTo)
    {
        buckets = state.Buckets!;
        if (buckets is null || buckets.Length == 0)
        {
            width = 0;
            clampedFrom = 0;
            clampedTo = 0;
            return false;
        }

        width = state.BucketWidthTicks;
        if (width <= 0)
        {
            clampedFrom = 0;
            clampedTo = 0;
            return false;
        }

        clampedFrom = Math.Max(fromTicks, state.WindowStartTicks);
        clampedTo = Math.Min(toTicks, state.WindowEndTicks);
        return clampedTo >= clampedFrom;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool BucketOverlaps(long bucketStart, long width, long fromInclusive, long toExclusive)
    {
        var bucketEnd = bucketStart + width;
        // overlap check: start < to && end > from
        return bucketStart < toExclusive && bucketEnd > fromInclusive;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AccumulateBucket(ref long count, ref double sum, ref double min, ref double max, in AggregateBucket b)
    {
        count += b.Count;
        sum += b.Sum;
        if (b.Min < min)
        {
            min = b.Min;
        }
        if (b.Max > max)
        {
            max = b.Max;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanUseBucketFastPath(DateTime from, DateTime to)
    {
        // Preconditions already checked by caller: _ts, WindowSizeTicks, ValueSelector present
        var fromTicks = from.Ticks;
        var toTicks = to.Ticks;
        if (toTicks < fromTicks)
        {
            return false;
        }

        var partitions = GetPartitionCount();
        for (var i = 0; i < partitions; i++)
        {
            ref var st = ref _windowStates[i];
            var buckets = st.Buckets;
            if (buckets is null || buckets.Length == 0)
            {
                return false;
            }
            if (st.BucketWidthTicks <= 0)
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Finds minimum value using a selector without allocations (double selector fast-path overload).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double? MinZeroAlloc(Func<TEvent, double> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);

        if (filter is null && from.HasValue && to.HasValue && _options.ValueSelector is not null && TimestampSelector is not null && _options.WindowSizeTicks.HasValue)
        {
            if (CanUseBucketFastPath(from.Value, to.Value))
            {
                var r = AggregateFromBuckets(from.Value, to.Value);
                return r.Count > 0 ? r.Min : null;
            }
        }
        return ZeroAllocationExtensions.MinZeroAlloc(this, selector, filter, from, to);
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

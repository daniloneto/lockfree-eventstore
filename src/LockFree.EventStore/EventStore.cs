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
    /// <summary>
    /// Initializes a new EventStore instance using the provided options.
    /// </summary>
    /// <param name="options">Configuration for the store. If null, defaults are used.</param>
    /// <remarks>
    /// The constructor validates options, allocates per-partition ring buffers (padded or standard depending on
    /// the false-sharing protection option), initializes statistics, key map, per-partition window state, and
    /// sets the public <see cref="TimestampSelector"/> from the options.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the configured number of partitions is less than or equal to zero.</exception>
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
    /// <summary>
    /// Attempts to retrieve a current snapshot of the store's statistics.
    /// </summary>
    /// <param name="stats">When this method returns, contains a snapshot of the current store statistics.</param>
    /// <returns>Always returns <c>true</c>. The return value is present for API compatibility and future extensibility.</returns>
    public bool TryGetStats(out StoreStats stats)
    {
        stats = GetCurrentStatsSnapshot();
        return true;
    }

    /// <summary>
    /// Creates a snapshot of current store statistics.
    /// <summary>
    /// Returns an atomic snapshot of the store's internal telemetry counters.
    /// </summary>
    /// <returns>A <see cref="StoreStats"/> containing the current append, dropped, snapshot-bytes-exposed, and window-advance counts.</returns>
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
    /// <summary>
    /// Invokes the configured OnStatsUpdated callback with a snapshot of current store statistics.
    /// </summary>
    /// <remarks>
    /// If an OnStatsUpdated handler is not configured this method does nothing. Any exception thrown
    /// by the user-provided callback is silently caught and ignored to avoid disrupting the store's
    /// normal operation.
    /// </remarks>
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
    /// <summary>
    /// Atomically increments the internal append counter and updates public statistics; optionally triggers a sampled stats update callback.
    /// </summary>
    /// <param name="delta">Number of appended items to account for (must be > 0). Defaults to 1. Batch values are supported.</param>
    /// <remarks>
    /// Side effects:
    /// - Atomically adds <paramref name="delta"/> to the internal append counter.
    /// - Updates the public Statistics.TotalAdded counter.
    /// - If an OnStatsUpdated handler is configured, may invoke a sampled notification according to Options.StatsUpdateInterval:
    ///   - If interval &lt;= 1 the notification is immediate.
    ///   - If interval is a power of two and delta == 1, uses a fast bitmask check.
    ///   - Otherwise uses a boundary-crossing check that supports batch increments.
    /// </remarks>
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
    /// <summary>
    /// Atomically increments the store's dropped-events counter by <paramref name="delta"/> and, if a stats update callback is configured, requests a stats notification.
    /// </summary>
    /// <param name="delta">Number of dropped events to add (defaults to 1).</param>
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
    /// <summary>
    /// Atomically adds <paramref name="delta"/> to the internal snapshot-bytes-exposed counter and triggers a stats update notification if an <see cref="EventStoreOptions{TEvent}.OnStatsUpdated"/> callback is configured.
    /// </summary>
    /// <param name="delta">Amount (in bytes) to add to the snapshot-bytes-exposed counter; may be negative to decrement.</param>
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
    /// <summary>
    /// Atomically increments the internal window-advance counter by <paramref name="delta"/> and triggers a stats update callback if one is configured.
    /// </summary>
    /// <param name="delta">Amount to add to the window-advance counter (defaults to 1).</param>
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
    /// <summary>
    /// Returns a snapshot of all registered key-to-KeyId mappings in the store.
    /// </summary>
    /// <returns>An <see cref="IReadOnlyDictionary{TKey, TValue}"/> mapping each registered key string to its <see cref="KeyId"/>.</returns>
    public IReadOnlyDictionary<string, KeyId> GetKeyMappings()
    {
        return _keyMap.GetAllMappings();
    }

    /// <summary>
    /// Internal discard callback used to update statistics and invoke user-provided hooks.
    /// </summary>
    /// <summary>
    /// Handles an event discarded by an internal ring buffer: updates store statistics and invokes user hooks.
    /// </summary>
    /// <param name="evt">The event instance that was discarded by a ring buffer.</param>
    /// <remarks>
    /// Increments internal dropped counters and records the discard in the public Statistics object.
    /// If an <see cref="EventStoreOptions{TEvent}.OnEventDiscarded"/> callback is configured it will be invoked with the discarded event.
    /// If the store reports full (<see cref="IsFull"/>), the configured <see cref="EventStoreOptions{TEvent}.OnCapacityReached"/> callback will be invoked.
    /// </remarks>
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

    /// <summary>
    /// Lazily initializes the per-partition rolling-window bucket ring in <paramref name="state"/> if windowing is enabled and buckets are not yet allocated.
    /// </summary>
    /// <remarks>
    /// - No-ops if <see cref="_options"/> does not specify <c>WindowSizeTicks</c>.
    /// - Determines <c>bucketCount</c> as at least 1 and computes <c>bucketWidth</c> from <c>BucketWidthTicks</c> or by dividing the window size by the bucket count (minimum 1).
    /// - Aligns bucket start times to <paramref name="currentTimestamp"/>, sets <c>WindowStartTicks</c> = <c>currentTimestamp - windowSize</c> and <c>WindowEndTicks</c> = <paramref name="currentTimestamp"/>, resets each bucket with its computed start, and clears partition aggregate state (Count, Sum, Min, Max).
    /// </remarks>
    /// <param name="partitionIndex">Partition index (currently unused; kept for future extensibility).</param>
    /// <param name="state">Reference to the partition's <see cref="PartitionWindowState"/> to initialize and mutate.</param>
    /// <param name="currentTimestamp">Current time expressed in the same tick units used by the configured timestamp selector (used to align the window).</param>
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

    /// <summary>
    /// Computes the mathematical modulo of <paramref name="x"/> by <paramref name="m"/>, returning a non‑negative remainder in the range [0, m-1].
    /// </summary>
    /// <param name="x">The dividend.</param>
    /// <param name="m">The modulus. Must be greater than zero.</param>
    /// <returns>The non‑negative remainder of <c>x</c> modulo <c>m</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Mod(int x, int m)
    {
        var r = x % m;
        return r < 0 ? r + m : r;
    }

    /// <summary>
    /// Aligns a timestamp down to the nearest multiple of <paramref name="width"/>.
    /// </summary>
    /// <param name="ticks">The timestamp (in ticks) to align.</param>
    /// <param name="width">The alignment interval (in ticks); must be non-zero.</param>
    /// <returns>The largest value &lt;= <paramref name="ticks"/> that is a multiple of <paramref name="width"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long AlignDown(long ticks, long width)
    {
        var rem = ((ticks % width) + width) % width;
        return ticks - rem;
    }

    /// <summary>
    /// Determines whether the partition's rolling time window should advance based on the provided timestamp.
    /// </summary>
    /// <remarks>
    /// This method requires a configured <see cref="TimestampSelector"/> and a window size (<see cref="_options.WindowSizeTicks"/>).
    /// It may lazily initialize the partition's bucket state as a side effect.
    /// </remarks>
    /// <param name="currentTimestamp">The current event timestamp (ticks) used to evaluate window advancement.</param>
    /// <param name="state">The partition's window state; may be initialized or mutated when buckets are lazily created.</param>
    /// <param name="newWindowStart">
    /// When the method returns <c>true</c>, contains the proposed new window start (computed as <c>currentTimestamp - windowSizeTicks</c>).
    /// When the method returns <c>false</c>, the value is undefined (initialized to 0 by the call).
    /// </param>
    /// <returns>
    /// <c>true</c> if the window should advance (caller must perform the advance); <c>false</c> if windowing is not enabled or no advance is needed.
    /// </returns>
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
    /// <summary>
    /// Advances the sliding time window for a single partition, rolling or resetting bucket slots as needed
    /// based on the provided current timestamp.
    /// </summary>
    /// <remarks>
    /// If the window does not need to move forward the method may extend only the window end.
    /// When the window advances across bucket boundaries this method will evict/roll bucket entries,
    /// reinitialize any newly exposed buckets (or reset all buckets if the advance exceeds the ring),
    /// and recompute the partition-level min/max aggregates. An internal window-advance counter is
    /// incremented for any non-noop advancement.
    /// </remarks>
    /// <param name="partitionIndex">Partition index (retained for future extensibility; not read).</param>
    /// <param name="state">Reference to the partition's window state; will be mutated to reflect the new window and bucket contents.</param>
    /// <param name="currentTimestamp">Current event timestamp in ticks used to determine window advancement and new window end.</param>
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

    /// <summary>
    /// Determines whether advancing the partition's rolling window would be a no-op.
    /// </summary>
    /// <param name="state">Current partition window state.</param>
    /// <param name="newWindowStart">Proposed new window start in ticks (aligned).</param>
    /// <param name="currentTimestamp">Current timestamp in ticks used to drive advancement.</param>
    /// <returns>
    /// True if the proposed start does not move the window forward (newWindowStart is not greater than the current start)
    /// and the partition's window end already equals the current timestamp; otherwise false.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNoopWindowAdvance(in PartitionWindowState state, long newWindowStart, long currentTimestamp)
    {
        return newWindowStart <= state.WindowStartTicks && currentTimestamp == state.WindowEndTicks;
    }

    /// <summary>
    /// Reinitializes all buckets in a partition's window state to align with a new window start.
    /// </summary>
    /// <remarks>
    /// Aligns the first bucket to the floor of <paramref name="newWindowStart"/> using the partition's bucket width,
    /// sets each bucket's start time consecutively, and resets per-partition aggregate counters.
    /// After this call the bucket ring is treated as empty and ready to accept new appends for the new window alignment.
    /// </remarks>
    /// <param name="state">The partition window state whose buckets and aggregates will be reset.</param>
    /// <param name="newWindowStart">The new window-start timestamp (ticks) used to compute bucket alignment.</param>
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

    /// <summary>
    /// Evicts the oldest buckets as the window advances and rolls the ring by the specified number of buckets.
    /// </summary>
    /// <remarks>
    /// This mutates <paramref name="state"/> in-place: it subtracts evicted buckets' counts and sums from the partition totals,
    /// advances <see cref="PartitionWindowState.BucketHead"/>, and resets the newly-created tail buckets so their start
    /// timestamps are aligned relative to <paramref name="newAlignedStart"/>. The operation may invalidate cached min/max
    /// aggregates for the partition; callers are responsible for recomputing those if needed.
    /// </remarks>
    /// <param name="state">The partition window state to update (modified in place).</param>
    /// <param name="advanceBuckets">Number of bucket positions to advance/evict from the head.</param>
    /// <param name="newAlignedStart">The aligned start timestamp (in ticks) of the new window used to compute tail bucket starts.</param>
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

    /// <summary>
    /// Recomputes the partition-level minimum and maximum values by scanning all non-empty buckets and updates the provided <paramref name="state"/>.
    /// </summary>
    /// <param name="state">Reference to the partition window state whose <see cref="PartitionWindowState.Min"/> and <see cref="PartitionWindowState.Max"/> will be updated. If the state has no events (<c>state.Count == 0</c>), Min is set to <c>double.MaxValue</c> and Max to <c>double.MinValue</c>.</param>
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

    /// <summary>
    /// Incorporates a single event's numeric value into the appropriate time bucket and the partition's running aggregate.
    /// </summary>
    /// <remarks>
    /// If the partition has no bucket array initialized, or if the event timestamp falls outside the partition's current window, the method returns without modifying state.
    /// The event is placed into the bucket corresponding to its offset from <c>state.WindowStartTicks</c>; if the computed bucket index would exceed the last bucket, it is clamped to the last bucket (inclusive end).
    /// </remarks>
    /// <param name="state">The partition's window state to update (contains buckets, head index, window bounds and aggregate counters).</param>
    /// <param name="eventTicks">The event timestamp expressed in ticks. Only events within [WindowStartTicks, WindowEndTicks] are considered.</param>
    /// <param name="value">The numeric value to add to the selected bucket and to the partition aggregate.</param>
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
    /// <summary>
    /// Validates that windowed bucket tracking is enabled when a time-range filter is supplied.
    /// </summary>
    /// <param name="from">Start of the time range (inclusive); null means unbounded start.</param>
    /// <param name="to">End of the time range (inclusive); null means unbounded end.</param>
    /// <exception cref="InvalidOperationException">Thrown if a time filter is provided while window tracking is disabled in options.</exception>
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
    /// <summary>
    /// Attempts to enqueue an event into the specified partition using the fast, non-windowed path.
    /// </summary>
    /// <param name="e">Event to append.</param>
    /// <param name="partition">Zero-based partition index to which the event will be enqueued.</param>
    /// <returns>True if the event was successfully enqueued; false if the target partition was full.</returns>
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
    /// <summary>
    /// Attempts to append an event into the specified partition and, if time-windowing is enabled,
    /// advances the partition's window and updates the corresponding bucket aggregates.
    /// </summary>
    /// <param name="e">The event to append.</param>
    /// <param name="partition">Index of the target partition.</param>
    /// <returns>True if the event was enqueued; false if the partition buffer rejected the item.</returns>
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
    /// <summary>
    /// Attempts to append a single event to the store, routing it to a partition and returning whether the append succeeded.
    /// </summary>
    /// <remarks>
    /// When TEvent is the built-in <c>Event</c> struct a hot fast-path is used that derives the partition from the event's KeyId
    /// (using a bitmask when the partition count is a power of two). For other event types the partition is determined by
    /// <c>Partitioners.ForKey</c>.
    /// </remarks>
    /// <param name="e">The event to append.</param>
    /// <returns><c>true</c> if the event was successfully enqueued; otherwise <c>false</c>.</returns>
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
    /// <summary>
    /// Attempts to append each event from the provided span to the store, skipping any that cannot be appended.
    /// </summary>
    /// <param name="batch">Span of events to append.</param>
    /// <returns>The number of events successfully appended from the span.</returns>
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
    /// <summary>
    /// Attempts to append a single event into the specified partition.
    /// </summary>
    /// <param name="partition">Zero-based partition index; must be in [0, GetPartitionCount() - 1].</param>
    /// <returns>True if the event was enqueued; false if the target partition was full.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="partition"/> is outside the valid partition range.</exception>
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
    /// <summary>
    /// Attempts to append an event to the store using a KeyId-based partitioning hot path.
    /// </summary>
    /// <param name="keyId">Key identifier used to select the target partition.</param>
    /// <param name="value">The event to append.</param>
    /// <param name="timestamp">Event timestamp (ticks) used for windowed aggregation when window tracking is enabled.</param>
    /// <returns>True if the event was successfully enqueued; false if the target partition was full and the event was not stored.</returns>
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
    /// <summary>
    /// Attempts to append each event from <paramref name="batch"/> in order, stopping on the first failed append.
    /// </summary>
    /// <param name="batch">Span of events to append.</param>
    /// <returns>The number of events successfully appended before a failure (or <c>batch.Length</c> if all succeeded).</returns>
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
    /// <summary>
    /// Retrieves the KeyId associated with the given key, creating and registering a new KeyId if the key is not already present.
    /// </summary>
    /// <param name="key">The string key to look up or register.</param>
    /// <returns>The existing or newly created <see cref="KeyId"/> mapped to <paramref name="key"/>.</returns>
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
    /// <summary>
    /// Attempts to append a sequence of events that are already keyed by KeyId.
    /// </summary>
    /// <param name="batch">A span of (KeyId, TEvent) tuples to append.</param>
    /// <returns>The number of events that were successfully appended.</returns>
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
    /// <summary>
    /// Appends a span of events to the store, routing each event to its partition and using an optimized path for small batches.
    /// </summary>
    /// <param name="batch">The events to append.</param>
    /// <returns>The number of events successfully appended (0 if <paramref name="batch"/> is empty).</returns>
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
    /// <summary>
    /// Attempts to append a span of events into the specified partition's buffer.
    /// </summary>
    /// <param name="batch">The events to append.</param>
    /// <param name="partition">Zero-based partition index to which the events will be appended.</param>
    /// <returns>The number of events successfully enqueued.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="partition"/> is outside the valid partition range.</exception>
    /// <remarks>
    /// On successful enqueues this method updates internal append telemetry and the public store statistics.
    /// </remarks>
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
    /// <summary>
    /// Determines whether the provided batch is considered "small" (uses the small-batch fast-path).
    /// </summary>
    /// <returns>True when the batch length is less than or equal to the small-batch threshold (32); otherwise false.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSmallBatch(ReadOnlySpan<TEvent> batch)
    {
        return batch.Length <= 32;
    }

    /// <summary>
    /// Appends a small batch using the standard per-item append path.
    /// </summary>
    /// <param name="batch">The batch of events to append.</param>
    /// <summary>
    /// Appends a small batch of events by attempting to append each event individually.
    /// </summary>
    /// <returns>The number of events that were successfully appended from the provided batch.</returns>
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
    /// <summary>
    /// Allocates a jagged array of per-partition buffers sized from <paramref name="partitionCounts"/>.
    /// </summary>
    /// <param name="partitionCounts">An array where each element is the desired length for the corresponding partition's buffer.</param>
    /// <returns>
    /// A jagged array with length equal to <paramref name="partitionCounts"/>. For each index i,
    /// the returned array contains a new <see cref="TEvent"/>[] of length <c>partitionCounts[i]</c> if that value is &gt; 0; otherwise the element is left null.
    /// </returns>
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
    /// <summary>
    /// Distributes events from <paramref name="batch"/> into per-partition buffers according to the partitioner.
    /// </summary>
    /// <param name="batch">Span of events to distribute.</param>
    /// <param name="partitionCount">Number of partitions; used by the partitioner to select target partition.</param>
    /// <param name="partitionArrays">Array of per-partition target arrays; each event is appended into the corresponding array slot.</param>
    /// <param name="partitionCounts">Mutable per-partition counters tracking the next write index; updated in-place as events are placed.</param>
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
    /// <summary>
    /// Appends per-partition event arrays to their corresponding partitions and updates append counters.
    /// </summary>
    /// <param name="partitionArrays">Jagged array where each element is the buffer of events destined for the partition at the same index. Elements may be null when a partition has no events.</param>
    /// <param name="partitionCounts">Parallel array of counts indicating how many items in each corresponding partition array are valid. Entries of zero are skipped.</param>
    /// <returns>The total number of events successfully enqueued across all partitions.</returns>
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
    /// <summary>
    /// Removes all events from every partition and resets the store's statistics.
    /// </summary>
    /// <remarks>
    /// After calling this, the store will be empty (per-partition buffers cleared) and the public <see cref="Statistics"/> will be reset to its initial state.
    /// </remarks>
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
    /// <summary>
    /// Removes all events whose timestamps are strictly older than <paramref name="olderThan"/>.
    /// </summary>
    /// <remarks>
    /// Requires a configured <c>TimestampSelector</c>. The method snapshots events that should be kept,
    /// clears all partitions, and re-enqueues the kept events so the store only contains events with
    /// timestamps >= <paramref name="olderThan"/> afterwards.
    /// </remarks>
    /// <param name="olderThan">Cutoff timestamp; events with timestamps before this value are purged.</param>
    /// <exception cref="InvalidOperationException">Thrown when no <c>TimestampSelector</c> is configured.</exception>
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

    /// <summary>
    /// Collects all events with a timestamp greater than or equal to <paramref name="olderThan"/> into a contiguous buffer.
    /// </summary>
    /// <param name="olderThan">Inclusive lower bound; events with timestamps &gt;= this value are kept.</param>
    /// <param name="pool">ArrayPool used when growing the buffer (omitted from parameter docs as a shared utility).</param>
    /// <param name="initialBuffer">An initial buffer to fill; may be returned unchanged or replaced with a pooled/rented buffer if resizing was required.</param>
    /// <returns>
    /// A tuple where <c>count</c> is the number of kept events and <c>buffer</c> is the array containing those events in indices [0, count).
    /// The returned buffer may be a pooled/rented array if resizing occurred; callers should return it to the pool when appropriate.
    /// </returns>
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

    /// <summary>
    /// Ensure an array has at least the requested capacity, renting a larger array from the pool if necessary.
    /// </summary>
    /// <remarks>
    /// If <paramref name="needed"/> is greater than <paramref name="buffer"/>'s length, a new array is rented from the provided pool
    /// with length at least max(buffer.Length * 2, needed). The contents of the old array are copied into the rented array
    /// and the old array is returned to the pool. The old array is cleared when returned if the element type requires clearing.
    /// </remarks>
    /// <param name="buffer">Current array to ensure capacity for; may be returned to the pool if a larger array is rented.</param>
    /// <param name="needed">Minimum required length for the returned array.</param>
    /// <returns>An array with length >= <paramref name="needed"/>. This will be the original <paramref name="buffer"/> when it was already large enough, otherwise a rented array from the pool.</returns>
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

    /// <summary>
    /// Clears every partition in the store and resets the public statistics object.
    /// </summary>
    /// <remarks>
    /// Invokes <see cref="ClearPartition(int)"/> for each partition returned by <see cref="GetPartitionCount()"/>,
    /// then calls <see cref="EventStoreStatistics.Reset()"/> on the store's <see cref="Statistics"/>.
    /// This is a private utility used to fully clear store contents and telemetry counters.
    /// </remarks>
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

    /// <summary>
    /// Re-enqueues the first <paramref name="count"/> events from <paramref name="buffer"/> into their partitions.
    /// </summary>
    /// <param name="buffer">Array containing events to re-add; only the first <paramref name="count"/> entries are used.</param>
    /// <param name="count">Number of events from <paramref name="buffer"/> to re-enqueue.</param>
    /// <remarks>
    /// Each event is routed to a partition using the configured partitioner. Enqueue failures are intentionally ignored.
    /// </remarks>
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
    /// <summary>
    /// Produces an immutable, ordered snapshot of all events across all partitions.
    /// </summary>
    /// <remarks>
    /// The snapshot reflects the store contents at the time of the call and is returned as a single array containing events in partition order. If the store is empty a shared empty list is returned to avoid allocation. The method updates the internal snapshot-bytes-exposed counter for telemetry.
    /// </remarks>
    /// <returns>An IReadOnlyList&lt;TEvent&gt; containing all events present in the store at the time of invocation (may be an empty shared list).</returns>
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
    /// <summary>
    /// Creates a materialized snapshot containing all events that satisfy the provided predicate.
    /// The returned list represents the state of the store at the time of the call and preserves partition order.
    /// </summary>
    /// <param name="filter">Predicate applied to each event; must not be null.</param>
    /// <returns>An IReadOnlyList&lt;TEvent&gt; containing the matching events.</returns>
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
    /// <summary>
    /// Returns a per-partition, zero-allocation view of the store's current contents.
    /// </summary>
    /// <returns>
    /// An array of <see cref="PartitionView{TEvent}"/>—one element per partition—representing a point-in-time view of each partition's events.
    /// The returned views do not copy event payloads; they reference partition buffers and reflect the state at the moment the view was created.
    /// </returns>
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
    /// <summary>
    /// Returns per-partition views of the store filtered to the specified time range.
    /// </summary>
    /// <param name="from">Inclusive lower bound of event time. If null, no lower bound is applied.</param>
    /// <param name="to">Inclusive upper bound of event time. If null, no upper bound is applied.</param>
    /// <returns>An array of <see cref="PartitionView{TEvent}"/>—one view per partition containing events whose timestamps fall within [from, to].</returns>
    /// <exception cref="InvalidOperationException">Thrown if <see cref="TimestampSelector"/> is not configured.</exception>
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
    /// <summary>
    /// Lazily enumerates all events currently stored, yielding each partition's events in partition order.
    /// </summary>
    /// <returns>An enumerable that streams the events from each partition in sequence. Enumeration is lazy and reflects the buffers' contents at enumeration time; it may observe concurrent additions or discards.</returns>
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
    /// <summary>
    /// Counts events in the store matching an optional filter and optional time window without allocating intermediate collections.
    /// </summary>
    /// <param name="filter">Optional predicate to include only events that satisfy the filter. If null, all events are considered.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps. Requires a configured <see cref="TimestampSelector"/> when set.</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps. Requires a configured <see cref="TimestampSelector"/> when set.</param>
    /// <returns>The total number of matching events.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long CountEventsZeroAlloc(EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.CountEventsZeroAlloc(this, filter, from, to);
    }

    /// <summary>
    /// Finds minimum value using a selector without allocations.
    /// <summary>
    /// Computes the minimum value produced by <paramref name="selector"/> over the store in a zero-allocation manner.
    /// Supports an optional predicate filter and optional time window; when a time window is provided, window tracking must be enabled.
    /// </summary>
    /// <typeparam name="TResult">Numeric or comparable value type returned by the selector.</typeparam>
    /// <param name="selector">Function that projects an event to the value to compare.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive start of the time range to consider.</param>
    /// <param name="to">Optional inclusive end of the time range to consider.</param>
    /// <returns>The minimum selected value across matching events, or <c>null</c> if no events match.</returns>
    /// <exception cref="InvalidOperationException">Thrown if a time range is supplied but timestamp/window tracking is not enabled.</exception>
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
    /// <summary>
    /// Computes a windowed aggregate (Count, Sum, Min, Max, Average) over stored events using a zero-allocation streaming path.
    /// </summary>
    /// <remarks>
    /// If possible this method uses a bucket-based fast path for the configured per-partition windows; otherwise it processes events in chunked, zero-allocation fashion.
    /// Time filtering requires a configured <see cref="TimestampSelector"/>; the method will validate that and throw if window tracking is required but not enabled.
    /// </remarks>
    /// <param name="selector">Function that maps an event to a double value used for aggregation.</param>
    /// <param name="filter">Optional predicate to include only matching events in the aggregation.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps (requires <see cref="TimestampSelector"/>).</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps (requires <see cref="TimestampSelector"/>).</param>
    /// <returns>A <see cref="WindowAggregateResult"/> containing Count, Sum, Min, Max and Average for the selected events.</returns>
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

    /// <summary>
    /// Attempts to perform a bucket-based fast-path window aggregation for the specified time range.
    /// </summary>
    /// <param name="filter">Must be null for the fast-path to be considered; any non-null filter disables the fast-path.</param>
    /// <param name="from">Start of the time range (must have a value for the fast-path).</param>
    /// <param name="to">End of the time range (must have a value for the fast-path).</param>
    /// <param name="result">When the method returns true, contains the aggregated window result; otherwise left as default.</param>
    /// <returns>True if the bucket fast-path was used and <paramref name="result"/> contains the aggregation; otherwise false.</returns>
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

    /// <summary>
    /// Accumulates aggregate metrics (count, sum, min, max) over a span of events using the provided selector.
    /// </summary>
    /// <param name="s">Current accumulator tuple; updated values are returned.</param>
    /// <param name="chunk">Span of events to process.</param>
    /// <param name="selector">Function that maps an event to a double value used for aggregation.</param>
    /// <returns>The updated accumulator tuple containing Count, Sum, Min, Max and a Has flag indicating whether any value was processed.</returns>
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

    /// <summary>
    /// Converts an aggregated tuple of windowed statistics into a <see cref="WindowAggregateResult"/>.
    /// </summary>
    /// <param name="s">Tuple containing aggregated values: <c>Count</c> (number of samples), <c>Sum</c>, <c>Min</c>, <c>Max</c>, and <c>Has</c> (whether any samples were observed).</param>
    /// <returns>
    /// A <see cref="WindowAggregateResult"/> with the provided statistics and computed average (Sum / Count) when <c>Has</c> is true;
    /// otherwise a zeroed <see cref="WindowAggregateResult"/>.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static WindowAggregateResult ToWindowAggregateResult((long Count, double Sum, double Min, double Max, bool Has) s)
    {
        return s.Has
            ? new WindowAggregateResult(s.Count, s.Sum, s.Min, s.Max, s.Sum / s.Count)
            : new WindowAggregateResult(0, 0.0, 0.0, 0.0, 0.0);
    }

    /// <summary>
    /// Finds maximum value using a selector without allocations (double selector fast-path overload).
    /// <summary>
    /// Computes the maximum value produced by <paramref name="selector"/> over stored events, optionally filtered and time-bounded, using a bucket-based fast path when available to avoid allocations.
    /// </summary>
    /// <param name="selector">Function that maps an event to a double value for comparison.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional start of the inclusive time range to consider.</param>
    /// <param name="to">Optional end of the inclusive time range to consider.</param>
    /// <returns>The maximum selected value, or <c>null</c> if no events match the filter/time range.</returns>
    /// <exception cref="InvalidOperationException">Thrown if a time range is provided but window tracking/timestamp selector is not configured.</exception>
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
    /// <summary>
    /// Returns the maximum value produced by <paramref name="selector"/> across events in the store using a zero-allocation streaming path.
    /// </summary>
    /// <param name="selector">Function that projects an event to a comparable value.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive lower time bound; requires a configured <see cref="TimestampSelector"/>.</param>
    /// <param name="to">Optional inclusive upper time bound; requires a configured <see cref="TimestampSelector"/>.</param>
    /// <returns>The maximum selected value, or <c>null</c> if no events match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TResult? MaxZeroAlloc<TResult>(Func<TEvent, TResult> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.MaxZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Sums numeric values using a double selector without allocations.
    /// <summary>
    /// Computes the sum of a projected double value across events without allocating intermediate collections.
    /// </summary>
    /// <remarks>
    /// When a time range is provided (both <paramref name="from"/> and <paramref name="to"/>), this method validates that window/bucket tracking is enabled and will use a partitioned bucket fast-path if all preconditions are met (no <paramref name="filter"/>, a configured value selector, a timestamp selector, and window size). Otherwise it falls back to the generic zero-allocation aggregation implementation.
    /// </remarks>
    /// <param name="selector">Function that projects an event to a double value to be summed.</param>
    /// <param name="filter">Optional predicate to filter events; when non-null the bucket fast-path is not used.</param>
    /// <param name="from">Optional inclusive start of the time window for the query. If provided, <paramref name="to"/> must also be provided to enable the bucket fast-path.</param>
    /// <param name="to">Optional inclusive end of the time window for the query. If provided, <paramref name="from"/> must also be provided to enable the bucket fast-path.</param>
    /// <returns>The sum of the projected values over the matched events (0.0 if no events match).</returns>
    /// <exception cref="InvalidOperationException">Thrown if a time-filtered query is requested but window/bucket tracking is not enabled.</exception>
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
    /// <summary>
    /// Computes the sum of values produced by <paramref name="selector"/> over the store's events without allocating temporary collections.
    /// </summary>
    /// <typeparam name="TValue">A numeric value type used for the sum (must implement <see cref="INumber{T}"/>).</typeparam>
    /// <param name="selector">A projection that maps an event to a numeric value to include in the sum.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive start time to restrict the sum to events with timestamps &gt;= this value.</param>
    /// <param name="to">Optional inclusive end time to restrict the sum to events with timestamps &lt;= this value.</param>
    /// <returns>The aggregated sum as <typeparamref name="TValue"/>.</returns>
    /// <exception cref="InvalidOperationException">Thrown if a time range is provided but the store has no configured timestamp selector / window tracking enabled.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TValue SumZeroAlloc<TValue>(Func<TEvent, TValue> selector, EventFilter<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        EnsureWindowTrackingEnabledIfTimeFiltered(from, to);
        return ZeroAllocationExtensions.SumZeroAlloc(this, selector, filter, from, to);
    }

    /// <summary>
    /// Computes average using a double selector without allocations.
    /// <summary>
    /// Computes the average of values produced by <paramref name="selector"/> over events in the store,
    /// optionally filtered by <paramref name="filter"/> and constrained to the time range [<paramref name="from"/>, <paramref name="to"/>].
    /// </summary>
    /// <param name="selector">Function that maps an event to a double value to include in the average.</param>
    /// <param name="filter">Optional event filter; when provided the bucket fast-path is not used.</param>
    /// <param name="from">Optional inclusive start of the time window for the query.</param>
    /// <param name="to">Optional inclusive end of the time window for the query.</param>
    /// <returns>The average of selector(event) for matching events. Returns 0.0 when no events match the query.</returns>
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
    /// <summary>
    /// Computes the average of values produced by <paramref name="selector"/> over the store's events using a zero-allocation streaming path.
    /// </summary>
    /// <param name="selector">Function that projects an event to a numeric value.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive start time to restrict the query; requires a configured <c>TimestampSelector</c> and enables window tracking when supplied.</param>
    /// <param name="to">Optional exclusive end time to restrict the query; requires a configured <c>TimestampSelector</c> and enables window tracking when supplied.</param>
    /// <returns>The average of the selected values as a <c>double</c>. Returns <c>double.NaN</c> if no events match the query.</returns>
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
    /// <summary>
    /// Iterates the snapshot of a single partition, applies a filter, buffers matching events into fixed-size chunks,
    /// and invokes the provided processor for each full chunk.
    /// </summary>
    /// <param name="index">Partition index to process.</param>
    /// <param name="filter">Predicate to select events to include.</param>
    /// <param name="processor">Action invoked with a ReadOnlySpan containing each full chunk of matching events.</param>
    /// <param name="buffer">Temporary buffer used to accumulate events before calling <paramref name="processor"/>.</param>
    /// <param name="count">Initial number of items already present in <paramref name="buffer"/>; returns the residual count after processing.</param>
    /// <param name="chunkSize">Number of items that constitute a full chunk and trigger <paramref name="processor"/>.</param>
    /// <returns>The number of items remaining in <paramref name="buffer"/> after processing the partition (a value in [0, <paramref name="chunkSize"/>)).</returns>
    /// <remarks>
    /// Each time a full chunk is submitted to <paramref name="processor"/>, the store's snapshot-bytes-exposed metric is incremented.
    /// </remarks>
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
    /// <summary>
    /// Returns the number of partitions currently configured, selecting the padded partition array when padding is enabled.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetPartitionCount()
    {
        return _usePadding ? _paddedPartitions!.Length : _partitions!.Length;
    }

    /// <summary>
    /// Helper to get approximate count from the appropriate partition type.
    /// <summary>
    /// Returns the approximate number of events in the specified partition, using the padded or standard buffer depending on store configuration.
    /// </summary>
    /// <param name="partitionIndex">Zero-based partition index.</param>
    /// <returns>Approximate count of events in the partition.</returns>
    private long GetPartitionCount(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].CountApprox : _partitions![partitionIndex].CountApprox;
    }

    /// <summary>
    /// Enumerates the snapshot of a partition.
    /// </summary>
    /// <param name="partitionIndex">The index of the partition.</param>
    /// <summary>
    /// Returns an enumerable snapshot of the events stored in the specified partition.
    /// </summary>
    /// <param name="partitionIndex">Index of the partition to enumerate. Must be a valid partition index for this store.</param>
    /// <returns>An IEnumerable&lt;TEvent&gt; that enumerates the events present in the partition at the time of the snapshot.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IEnumerable<TEvent> EnumeratePartitionSnapshot(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].EnumerateSnapshot() : _partitions![partitionIndex].EnumerateSnapshot();
    }

    /// <summary>
    /// Helper to get capacity from the appropriate partition type.
    /// <summary>
    /// Returns the configured capacity of the specified partition.
    /// </summary>
    /// <param name="partitionIndex">Zero-based partition index.</param>
    /// <returns>The capacity of the partition at the given index.</returns>
    private int GetPartitionCapacity(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].Capacity : _partitions![partitionIndex].Capacity;
    }

    /// <summary>
    /// Helper to try enqueue to the appropriate partition type.
    /// <summary>
    /// Attempts to enqueue an event into the specified partition's ring buffer.
    /// Selects the padded or standard partition array based on the store's padding setting.
    /// </summary>
    /// <param name="partitionIndex">Zero-based index of the target partition.</param>
    /// <param name="item">The event to enqueue.</param>
    /// <returns>True if the item was successfully enqueued; false if the target partition's buffer was full.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryEnqueueToPartition(int partitionIndex, TEvent item)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].TryEnqueue(item) : _partitions![partitionIndex].TryEnqueue(item);
    }

    /// <summary>
    /// Helper to try enqueue batch to the appropriate partition type.
    /// <summary>
    /// Attempts to enqueue a batch of events into the specified partition.
    /// </summary>
    /// <param name="partitionIndex">Zero-based index of the target partition.</param>
    /// <param name="items">Span containing the events to enqueue.</param>
    /// <returns>The number of items successfully enqueued.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int TryEnqueueBatchToPartition(int partitionIndex, ReadOnlySpan<TEvent> items)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].TryEnqueueBatch(items) : _partitions![partitionIndex].TryEnqueueBatch(items);
    }

    /// <summary>
    /// Helper to create view from the appropriate partition type.
    /// <summary>
    /// Returns a view over the specified partition's buffer, using the padded buffer type when padding is enabled.
    /// </summary>
    /// <param name="partitionIndex">Zero-based index of the partition to view.</param>
    /// <returns>A <see cref="PartitionView{TEvent}"/> for the requested partition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<TEvent> CreatePartitionView(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].CreateView(TimestampSelector) : _partitions![partitionIndex].CreateView(TimestampSelector);
    }

    /// <summary>
    /// Helper to create filtered view from the appropriate partition type.
    /// <summary>
    /// Create a time-filtered view of a single partition, using the configured buffer type (padded or standard).
    /// </summary>
    /// <param name="partitionIndex">Index of the partition to view.</param>
    /// <param name="fromTicks">Inclusive lower bound of the timestamp range (ticks).</param>
    /// <param name="toTicks">Exclusive upper bound of the timestamp range (ticks).</param>
    /// <param name="timestampSelector">Selector used to extract timestamps from events for filtering.</param>
    /// <returns>A <see cref="PartitionView{TEvent}"/> representing events in the specified time range for the partition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<TEvent> CreatePartitionViewFiltered(int partitionIndex, long fromTicks, long toTicks, IEventTimestampSelector<TEvent> timestampSelector)
    {
        return _usePadding ?
            _paddedPartitions![partitionIndex].CreateViewFiltered(fromTicks, toTicks, timestampSelector) :
            _partitions![partitionIndex].CreateViewFiltered(fromTicks, toTicks, timestampSelector);
    }

    /// <summary>
    /// Helper to clear the appropriate partition type.
    /// <summary>
    /// Clears all events from the partition at the given zero-based index (uses padded or standard buffer depending on configuration).
    /// </summary>
    /// <param name="partitionIndex">Zero-based partition index identifying which partition to clear.</param>
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
    /// <summary>
    /// Returns true if the specified partition currently contains no events.
    /// </summary>
    /// <param name="partitionIndex">Zero-based partition index to check.</param>
    /// <returns>True when the partition is empty; otherwise false.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsPartitionEmpty(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].IsEmpty : _partitions![partitionIndex].IsEmpty;
    }

    /// <summary>
    /// Helper to check if a partition is full.
    /// <summary>
    /// Checks whether the specified partition is currently full.
    /// </summary>
    /// <param name="partitionIndex">Zero-based index of the partition to check.</param>
    /// <returns>true if the partition is full; otherwise false.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsPartitionFull(int partitionIndex)
    {
        return _usePadding ? _paddedPartitions![partitionIndex].IsFull : _partitions![partitionIndex].IsFull;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// Processes results in fixed-size chunks to avoid large allocations.
    /// <summary>
    /// Streams all events across partitions to the given processor in partition order using fixed-size spans to avoid allocations.
    /// </summary>
    /// <param name="processor">Action invoked for each chunk as a ReadOnlySpan&lt;TEvent&gt;. The processor is executed synchronously for each chunk and must complete before the method continues.</param>
    /// <param name="chunkSize">Maximum number of events provided to the processor in a single span (default: Buffers.DefaultChunkSize).</param>
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
    /// <summary>
    /// Streams the padded partition at <paramref name="index"/> to <paramref name="processor"/> in chunks using a zero-allocation snapshot.
    /// </summary>
    /// <param name="index">Partition index to snapshot (padded buffer).</param>
    /// <param name="processor">Called for each non-empty chunk as a <see cref="ReadOnlySpan{T}"/>; invoked on the calling thread.</param>
    /// <param name="chunkSize">Preferred chunk size for rented buffers passed to <paramref name="processor"/>.</param>
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
    /// <summary>
    /// Streams the zero-allocation snapshot for a standard (non-padded) partition in fixed-size chunks,
    /// invoking <paramref name="processor"/> for each chunk.
    /// </summary>
    /// <param name="index">Index of the partition to snapshot. Must refer to an initialized standard partition.</param>
    /// <param name="processor">Action invoked for each chunk; receives a read-only span of events for that chunk.</param>
    /// <param name="chunkSize">Preferred chunk size (number of elements) used when producing spans to <paramref name="processor"/>.</param>
    /// <exception cref="InvalidOperationException">Thrown if the requested partition is not initialized.</exception>
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
    /// <summary>
    /// Streams a time-agnostic, filtered snapshot of all events to a consumer in fixed-size, zero-allocation chunks.
    /// </summary>
    /// <remarks>
    /// The method evaluates <paramref name="filter"/> for each event and invokes <paramref name="processor"/> with a ReadOnlySpan containing up to <paramref name="chunkSize"/> matching events.
    /// The implementation reuses a pooled buffer; callers MUST NOT retain or store the span beyond the lifetime of the processor call.
    /// Calls to <paramref name="processor"/> may occur multiple times (including zero times if no events match).
    /// Snapshot exposure is recorded for telemetry.
    /// </remarks>
    /// <param name="filter">Predicate used to select which events to include in the snapshot.</param>
    /// <param name="processor">Action invoked for each chunk of matching events. Receives a ReadOnlySpan of events to process immediately.</param>
    /// <param name="chunkSize">Preferred maximum chunk size passed to the processor. Defaults to Buffers.DefaultChunkSize.</param>
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
    /// <summary>
    /// Streams time-filtered, zero-allocation snapshots of events to the provided processor in fixed-size chunks.
    /// </summary>
    /// <remarks>
    /// Iterates each partition, obtains a filtered, zero-allocation partition view (using the configured <see cref="TimestampSelector"/>),
    /// and invokes <paramref name="processor"/> with consecutive spans of up to <paramref name="chunkSize"/> events. This method does not allocate per-event buffers and emits segments for both the contiguous and wrap-around portions of each partition view.
    /// </remarks>
    /// <param name="from">Inclusive lower bound of the timestamp filter; pass null for no lower bound.</param>
    /// <param name="to">Inclusive upper bound of the timestamp filter; pass null for no upper bound.</param>
    /// <param name="processor">Callback invoked for each chunk of events (as a <see cref="ReadOnlySpan{T}"/>).</param>
    /// <param name="chunkSize">Maximum number of events supplied to <paramref name="processor"/> per invocation.</param>
    /// <exception cref="InvalidOperationException">Thrown if no <see cref="TimestampSelector"/> is configured (time filtering requires a timestamp selector).</exception>
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
    /// <summary>
    /// Computes a windowed aggregate over the configured bucketed windows for the given time range.
    /// </summary>
    /// <param name="from">Start of the time range. If this is later than <paramref name="to"/>, the two values are swapped to form a valid range.</param>
    /// <param name="to">End of the time range. If this is earlier than <paramref name="from"/>, the two values are swapped to form a valid range.</param>
    /// <returns>A <see cref="WindowAggregateResult"/> representing the merged aggregation across all partitions for the specified time interval.</returns>
    /// <exception cref="InvalidOperationException">Thrown if window tracking is not enabled in the store options.</exception>
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

    /// <summary>
    /// Aggregates bucketed window statistics for a single partition over the specified time range (ticks).
    /// </summary>
    /// <param name="state">Partition window state containing the bucket ring to scan (may be prepared/validated by <c>TryPrepareBucketScan</c>).</param>
    /// <param name="fromTicks">Inclusive lower bound of the time range, expressed in ticks.</param>
    /// <param name="toTicks">Upper bound of the time range, expressed in ticks; treated as an exclusive end when scanning buckets.</param>
    /// <returns>
    /// A <see cref="WindowAggregateState"/> containing the aggregated Count, Sum, Min and Max computed from all buckets that overlap the requested time range.
    /// If no buckets overlap the range (or the scan cannot be prepared), an empty/default state (zeros and extrema set to defaults) is returned.
    /// </returns>
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

    /// <summary>
    /// Validates and prepares parameters for scanning partition buckets over a time range.
    /// </summary>
    /// <param name="state">The partition's window state containing bucket ring, window bounds, and bucket width.</param>
    /// <param name="fromTicks">Requested inclusive start time in ticks.</param>
    /// <param name="toTicks">Requested inclusive end time in ticks.</param>
    /// <param name="buckets">Outputs the partition's bucket array (may be null/empty if not initialized).</param>
    /// <param name="width">Outputs the bucket width in ticks.</param>
    /// <param name="clampedFrom">Outputs the requested start clamped to the partition's current window start.</param>
    /// <param name="clampedTo">Outputs the requested end clamped to the partition's current window end.</param>
    /// <returns>True if a valid, non-empty bucket range exists for the clamped interval; otherwise false.</returns>
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

    /// <summary>
    /// Determines whether a time bucket (starting at <paramref name="bucketStart"/> with length <paramref name="width"/>)
    /// overlaps the half-open time range [<paramref name="fromInclusive"/>, <paramref name="toExclusive"/>).
    /// </summary>
    /// <param name="bucketStart">Start of the bucket (ticks).</param>
    /// <param name="width">Width/length of the bucket (ticks).</param>
    /// <param name="fromInclusive">Inclusive start of the query range (ticks).</param>
    /// <param name="toExclusive">Exclusive end of the query range (ticks).</param>
    /// <returns>True if the bucket and the range overlap; otherwise false.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool BucketOverlaps(long bucketStart, long width, long fromInclusive, long toExclusive)
    {
        var bucketEnd = bucketStart + width;
        // overlap check: start < to && end > from
        return bucketStart < toExclusive && bucketEnd > fromInclusive;
    }

    /// <summary>
    /// Merges a single bucket's aggregates into running aggregate accumulators.
    /// </summary>
    /// <param name="count">Reference to the running event count; incremented by the bucket's count.</param>
    /// <param name="sum">Reference to the running sum; increased by the bucket's sum.</param>
    /// <param name="min">Reference to the running minimum; replaced if the bucket's minimum is smaller.</param>
    /// <param name="max">Reference to the running maximum; replaced if the bucket's maximum is larger.</param>
    /// <param name="b">The bucket whose aggregates are merged into the running accumulators.</param>
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

    /// <summary>
    /// Determines whether the optimized bucket-based aggregation fast path can be used for the specified time range.
    /// </summary>
    /// <param name="from">Start of the time range (inclusive).</param>
    /// <param name="to">End of the time range (inclusive).</param>
    /// <returns>
    /// True if the time range is valid (from <= to) and every partition has an initialized non-empty bucket array with a positive bucket width; otherwise false.
    /// </returns>
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
    /// <summary>
    /// Computes the minimum value produced by <paramref name="selector"/> across stored events, using a zero-allocation scan.
    /// When a time range and value/timestamp selectors and windowing are configured, a bucket-based fast-path is used.
    /// </summary>
    /// <param name="selector">Function that projects an event to a double value for comparison.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive start of a time window to query.</param>
    /// <param name="to">Optional inclusive end of a time window to query.</param>
    /// <returns>The minimum projected value, or <c>null</c> if no events match the criteria.</returns>
    /// <exception cref="InvalidOperationException">Thrown if a time range is provided but window tracking/timestamp configuration required for time-filtered queries is not enabled.</exception>
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

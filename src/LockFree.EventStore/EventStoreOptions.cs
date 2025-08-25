namespace LockFree.EventStore;

/// <summary>
/// Configuration for <see cref="EventStore{TEvent}"/>.
/// </summary>
public sealed class EventStoreOptions<TEvent>
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
    public int? Capacity { get; init; }

    /// <summary>
    /// Optional timestamp selector used for temporal queries.
    /// </summary>
    public IEventTimestampSelector<TEvent>? TimestampSelector { get; init; }

    /// <summary>
    /// Optional numeric value selector used to enable fast typed window aggregations (count/sum/min/max/avg).
    /// When provided together with <see cref="WindowSizeTicks"/>, the store maintains per-partition bucketed aggregates
    /// updated on append, enabling O(1) evict/apply during window advance.
    /// </summary>
    public Func<TEvent, double>? ValueSelector { get; init; }

    /// <summary>
    /// Optional callback invoked when an event is discarded due to capacity limits.
    /// </summary>
    public Action<TEvent>? OnEventDiscarded { get; init; }

    /// <summary>
    /// Optional callback invoked when the store reaches capacity.
    /// </summary>
    public Action? OnCapacityReached { get; init; }

    /// <summary>
    /// Optional callback invoked when telemetry statistics are updated.
    /// Provides access to current store statistics after relevant counter updates.
    /// </summary>
    public Action<StoreStats>? OnStatsUpdated { get; init; }

    /// <summary>
    /// Enables anti-false sharing padding for partition metadata.
    /// Improves performance in high-contention MPMC scenarios at the cost of memory usage.
    /// Default: false for compatibility.
    /// </summary>
    public bool EnableFalseSharingProtection { get; init; }

    /// <summary>
    /// Window size in ticks for incremental window aggregation.
    /// Defaults to 5 minutes if not specified.
    /// </summary>
    public long? WindowSizeTicks { get; init; }

    /// <summary>
    /// Number of time buckets kept per partition for fast window aggregations. Default: 512.
    /// Total window coverage is approximately <see cref="BucketCount"/> * <see cref="BucketWidthTicks"/>.
    /// </summary>
    public int BucketCount { get; init; } = 512;

    /// <summary>
    /// Width of each time bucket in ticks. If not specified, it will be derived from WindowSizeTicks / BucketCount.
    /// </summary>
    public long? BucketWidthTicks { get; init; }

    /// <summary>
    /// Enables or disables runtime window tracking (bucket maintenance on append).
    /// Default: true. When false, appends bypass all window/bucket logic and window aggregations are unavailable.
    /// </summary>
    public bool EnableWindowTracking { get; init; } = true;

    /// <summary>
    /// Interval for sampling stats notifications on appends. Notify every N appends. Default: 1.
    /// Set to 1 to notify on every append (legacy behavior).
    /// For high-throughput scenarios, prefer a power-of-two value like 1024, 2048 or 4096 so the check uses a fast bitmask.
    /// </summary>
    public int StatsUpdateInterval { get; init; } = 1;

    /// <summary>
    /// Gets the effective total capacity.
    /// <summary>
    /// Gets the effective total capacity for the event store.
    /// </summary>
    /// <returns>The total number of slots: <see cref="Capacity"/> if set; otherwise <c>CapacityPerPartition * Partitions</c>.</returns>
    public int GetTotalCapacity()
    {
        return Capacity ?? (CapacityPerPartition * Partitions);
    }

    /// <summary>
    /// Validates bucket-related configuration to fail fast on invalid settings.
    /// <summary>
    /// Validates windowing and stats-related configuration and throws if any setting is invalid.
    /// </summary>
    /// <remarks>
    /// Performs fast-fail checks for bucket/window configuration and stats sampling:
    /// - BucketCount must be > 0.
    /// - If set, BucketWidthTicks must be > 0.
    /// - If both WindowSizeTicks and BucketWidthTicks are set, WindowSizeTicks must be >= BucketWidthTicks and an exact multiple of BucketWidthTicks.
    /// - StatsUpdateInterval must be > 0.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when BucketCount &lt;= 0, when BucketWidthTicks is specified but &lt;= 0, or when StatsUpdateInterval &lt;= 0.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when both WindowSizeTicks and BucketWidthTicks are specified but WindowSizeTicks is smaller than BucketWidthTicks or not an exact multiple of it.
    /// </exception>
    public void Validate()
    {
        if (BucketCount <= 0)
        {
            // Use ArgumentOutOfRangeException with null paramName to avoid referencing non-parameters
            throw new ArgumentOutOfRangeException(null, "BucketCount must be greater than zero.");
        }

        if (BucketWidthTicks.HasValue && BucketWidthTicks.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(null, "BucketWidthTicks must be greater than zero when specified.");
        }

        if (WindowSizeTicks.HasValue && BucketWidthTicks.HasValue)
        {
            var window = WindowSizeTicks.Value;
            var width = BucketWidthTicks.Value;
            if (window < width)
            {
                throw new ArgumentException("WindowSizeTicks must be greater than or equal to BucketWidthTicks when both are specified.");
            }
            if (window % width != 0)
            {
                throw new ArgumentException("WindowSizeTicks must be an exact multiple of BucketWidthTicks when both are specified.");
            }
        }

        if (StatsUpdateInterval <= 0)
        {
            throw new ArgumentOutOfRangeException(null, "StatsUpdateInterval must be greater than zero.");
        }
    }
}

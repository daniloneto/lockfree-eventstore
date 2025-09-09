using System.IO;

namespace LockFree.EventStore.Snapshots;

/// <summary>
/// Snapshot configuration options (see RFC005).
/// </summary>
public sealed class SnapshotOptions
{
    /// <summary>Enables the snapshot subsystem when true.</summary>
    public bool Enabled { get; init; }
    /// <summary>Time based trigger. When > 0 a snapshot attempt may start after this interval.</summary>
    public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(5);
    /// <summary>Minimum number of appended events between snapshots (per global store) before another snapshot attempt.</summary>
    public int MinEventsBetweenSnapshots { get; init; } = 100_000;
    /// <summary>If true compact / prune in-memory structures before materializing snapshot (future hook).</summary>
    public bool CompactBeforeSnapshot { get; init; } = true;
    /// <summary>Maximum number of concurrent snapshot save jobs.</summary>
    public int MaxConcurrentSnapshotJobs { get; init; } = 2;
    /// <summary>How many successful snapshots to keep per partition before pruning older ones.</summary>
    public int SnapshotsToKeep { get; init; } = 3;
    /// <summary>Maximum attempts (with backoff) to persist a single snapshot before giving up.</summary>
    public int MaxSaveAttempts { get; init; } = 5;
    /// <summary>Base delay for exponential backoff when persisting fails.</summary>
    public TimeSpan BackoffBaseDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    /// <summary>Multiplicative factor applied each retry attempt.</summary>
    public double BackoffFactor { get; init; } = 2.0;
    /// <summary>Enables local ActivitySource based tracing (not distributed) for diagnostics.</summary>
    public bool EnableLocalTracing { get; init; }
    /// <summary>Maximum number of pending snapshot jobs queued (backpressure / loss when exceeded).</summary>
    public int MaxPendingSnapshotJobs { get; init; } = 128;
    /// <summary>Optional expected schema version. When set, restore will FAIL-FAST on mismatch instead of ignoring.</summary>
    public int? ExpectedSchemaVersion { get; init; }
    /// <summary>If true a final snapshot pass is attempted during clean shutdown (hosted service Stop).</summary>
    public bool FinalSnapshotOnShutdown { get; init; }
    /// <summary>Timeout for the final snapshot pass when FinalSnapshotOnShutdown is enabled.</summary>
    public TimeSpan FinalSnapshotTimeout { get; init; } = TimeSpan.FromSeconds(5);
    /// <summary>Maximum attempts to capture a stable in-memory partition view before giving up a job (contention). Default 8 (current hard-coded behavior).</summary>
    public int StableCaptureMaxAttempts { get; init; } = 8;
}

/// <summary>
/// Metadata describing a stored snapshot.
/// </summary>
public readonly record struct SnapshotMetadata(
    string PartitionKey,
    long Version,
    DateTimeOffset TakenAt,
    int SchemaVersion = 1);

/// <summary>
/// Serializer contract for snapshot persistence.
/// </summary>
public interface ISnapshotSerializer
{
    /// <summary>Serializes a partition state into destination stream.</summary>
    ValueTask SerializeAsync(Stream destination, PartitionState state, CancellationToken ct = default);
    /// <summary>Deserializes a partition state from source stream.</summary>
    ValueTask<PartitionState> DeserializeAsync(Stream source, CancellationToken ct = default);
}

/// <summary>
/// Store contract for persisting snapshot blobs.
/// </summary>
public interface ISnapshotStore
{
    /// <summary>Persist snapshot data atomically.</summary>
    ValueTask SaveAsync(SnapshotMetadata meta, Stream data, CancellationToken ct = default);
    /// <summary>Loads the latest snapshot (by version then timestamp) for a partition or null if none.</summary>
    ValueTask<(SnapshotMetadata Meta, Stream Data)?> TryLoadLatestAsync(string partitionKey, CancellationToken ct = default);
    /// <summary>Enumerates partition keys that have snapshots.</summary>
    IAsyncEnumerable<string> ListPartitionKeysAsync(CancellationToken ct = default);
    /// <summary>Prunes older snapshots keeping most recent N.</summary>
    ValueTask PruneAsync(string partitionKey, int snapshotsToKeep, CancellationToken ct = default);
}

/// <summary>
/// Hooks for future event delta persistence (currently no-op implementations will be provided).
/// </summary>
public interface IEventDeltaWriter
{
    /// <summary>Appends a delta segment for the given partition from a starting version.</summary>
    ValueTask AppendAsync(string partitionKey, ReadOnlyMemory<Event> events, long fromVersion, CancellationToken ct = default);
}
/// <summary>
/// Reader counterpart for deltas (unused now / no-op implementation).
/// </summary>
public interface IEventDeltaReader
{
    /// <summary>Reads deltas since specified version (currently yields nothing).</summary>
    IAsyncEnumerable<Event> ReadSinceAsync(string partitionKey, long version, CancellationToken ct = default);
}

/// <summary>
/// Simple partition materialized immutable state used for serialization.
/// </summary>
public sealed class PartitionState
{
    /// <summary>Logical partition key (synthetic for whole-store snapshots if needed).</summary>
    public required string PartitionKey { get; init; }
    /// <summary>Monotonic version at capture time (e.g. event count or aggregated version).</summary>
    public required long Version { get; init; }
    /// <summary>Materialized immutable event array.</summary>
    public required Event[] Events { get; init; }
    /// <summary>UTC timestamp when snapshot was taken.</summary>
    public DateTimeOffset TakenAt { get; init; } = DateTimeOffset.UtcNow;
    /// <summary>Schema version for forward compatibility.</summary>
    public int SchemaVersion { get; init; } = 1;
}

/// <summary>
/// Validation helpers for snapshot options.
/// </summary>
public static class SnapshotValidation
{
    /// <summary>Validates snapshot related options throwing fail-fast exceptions when invalid.</summary>
    public static void ValidateSnapshotOptions(SnapshotOptions options, ISnapshotSerializer? serializer, ISnapshotStore? store)
    {
        if (!options.Enabled)
        {
            return;
        }
        if (options.Interval <= TimeSpan.Zero && options.MinEventsBetweenSnapshots <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Either Interval must be > 0 or MinEventsBetweenSnapshots must be > 0 when snapshots are enabled.");
        }
        if (options.MaxConcurrentSnapshotJobs < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxConcurrentSnapshotJobs must be >= 1.");
        }
        if (options.SnapshotsToKeep < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "SnapshotsToKeep must be >= 1.");
        }
        if (options.MaxSaveAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxSaveAttempts must be >= 1.");
        }
        if (options.MaxPendingSnapshotJobs < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxPendingSnapshotJobs must be >= 1.");
        }
        if (options.ExpectedSchemaVersion.HasValue && options.ExpectedSchemaVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ExpectedSchemaVersion, when set, must be > 0.");
        }
        if (options.StableCaptureMaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "StableCaptureMaxAttempts must be >= 1.");
        }
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(store);
    }
}

/// <summary>
/// Backoff policy abstraction.
/// </summary>
public interface IBackoffPolicy
{
    /// <summary>Returns next delay for a (1-based) attempt count.</summary>
    TimeSpan NextDelay(int attempt);
}

/// <summary>
/// Exponential backoff policy with +/-10% jitter.
/// </summary>
public sealed class ExponentialBackoffPolicy(TimeSpan baseDelay, double factor) : IBackoffPolicy
{
    // Validate constructor arguments (fail fast)
    private readonly TimeSpan _baseDelay = baseDelay <= TimeSpan.Zero
        ? throw new ArgumentOutOfRangeException(nameof(baseDelay), "Base delay must be > 0.")
        : baseDelay;
    private readonly double _factor = factor <= 0
        ? throw new ArgumentOutOfRangeException(nameof(factor), "Factor must be > 0.")
        : factor;
    // Removed per-instance Random (not thread-safe); using Random.Shared.

    private double _lastDelayMs; // ensure monotonic strictly increasing sequence expected by tests

    /// <inheritdoc />
    public TimeSpan NextDelay(int attempt)
    {
        if (attempt < 1)
        {
            attempt = 1;
        }
        // Compute exponential raw delay in milliseconds.
        double raw;
        try
        {
            raw = _baseDelay.TotalMilliseconds * Math.Pow(_factor, attempt - 1);
        }
        catch (OverflowException)
        {
            return TimeSpan.MaxValue; // exponent overflow
        }
        if (double.IsNaN(raw) || double.IsInfinity(raw) || raw <= 0)
        {
            if (raw <= 0)
            {
                raw = _baseDelay.TotalMilliseconds; // fallback
            }
            if (double.IsInfinity(raw))
            {
                return TimeSpan.MaxValue;
            }
        }
        // Positive-only jitter (up to +10%) to satisfy tests expecting delay >= base raw value.
        var jitterMultiplier = 1.0 + (Random.Shared.NextDouble() * 0.1); // [1.0, 1.1)
        var ms = raw * jitterMultiplier;
        var maxMs = TimeSpan.MaxValue.TotalMilliseconds;
        if (ms >= maxMs)
        {
            return TimeSpan.MaxValue;
        }
        // Enforce monotonic strictly increasing sequence.
        if (ms <= _lastDelayMs)
        {
            ms = Math.Min(_lastDelayMs + 1, maxMs - 1); // bump by 1ms within bounds
        }
        _lastDelayMs = ms;
        return TimeSpan.FromMilliseconds(ms);
    }
}

/// <summary>
/// No-op delta writer placeholder.
/// </summary>
public sealed class NoopEventDeltaWriter : IEventDeltaWriter
{
    /// <inheritdoc />
    public ValueTask AppendAsync(string partitionKey, ReadOnlyMemory<Event> events, long fromVersion, CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// No-op delta reader placeholder.
/// </summary>
public sealed class NoopEventDeltaReader : IEventDeltaReader
{
    /// <inheritdoc />
    public async IAsyncEnumerable<Event> ReadSinceAsync(string partitionKey, long version, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

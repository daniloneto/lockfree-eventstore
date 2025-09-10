using System.Collections.Concurrent;
using System.IO;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LockFree.EventStore.Snapshots;

/// <summary>
/// Coordinates periodic snapshot jobs with concurrency limits, retry and pruning.
/// </summary>
public sealed class Snapshotter : IDisposable
{
    private readonly EventStore<Event> _store;
    private readonly ISnapshotSerializer _serializer;
    private readonly ISnapshotStore _storeBackend;
    private readonly IBackoffPolicy _backoff;
    private readonly ILogger? _logger;
    private readonly ConcurrentQueue<string> _jobQueue = new();
    private readonly SemaphoreSlim _concurrency;
    private readonly ConcurrentDictionary<Task, byte> _runningJobs = new(); // track in-flight snapshot jobs for graceful shutdown
    private long _lastSnapshotTicks;
    private long _eventsSinceLast;
    private readonly ActivitySource? _activity;
    private long _droppedJobs; // backpressure counter (total dropped enqueue attempts)
    private readonly ConcurrentDictionary<string, PartitionSnapshotInternal> _partitionStatus = new();
    private long _totalFailed; // total failed snapshot jobs (after max attempts)
    private long _stableCaptureFailures; // total times stable copy failed (contention)

    private static readonly Action<ILogger, Exception> _logLoopError = LoggerMessage.Define(LogLevel.Error, new EventId(1001, nameof(Snapshotter) + ":LoopError"), "Snapshot loop error");
    private static readonly Action<ILogger, int, double, Exception?> _logSaveRetry = LoggerMessage.Define<int, double>(LogLevel.Warning, new EventId(1002, nameof(Snapshotter) + ":SaveRetry"), "Snapshot save failed attempt {Attempt}, retrying in {Delay}ms");
    private static readonly Action<ILogger, int, Exception> _logSaveFailed = LoggerMessage.Define<int>(LogLevel.Error, new EventId(1003, nameof(Snapshotter) + ":SaveFailed"), "Snapshot save failed after {Attempts} attempts");
    private static readonly Action<ILogger, int, Exception?> _logJobDropped = LoggerMessage.Define<int>(LogLevel.Debug, new EventId(1004, nameof(Snapshotter) + ":JobDropped"), "Snapshot job dropped (pending queue size reached limit {Limit})");
    private static readonly Action<ILogger, string, Exception?> _logStableFail = LoggerMessage.Define<string>(LogLevel.Debug, new EventId(1005, nameof(Snapshotter) + ":StableCaptureFailed"), "Stable view capture failed for partition {Partition} (will retry later)");
    private static readonly Action<ILogger, string, Exception?> _logPruneFailed = LoggerMessage.Define<string>(LogLevel.Warning, new EventId(1006, nameof(Snapshotter) + ":PruneFailed"), "Snapshot pruning failed for partition {Partition}");

    private sealed class PartitionSnapshotInternal
    {
        public long LastVersion;
        public DateTimeOffset LastSnapshotAt;
        public long EventsSinceLastSnapshot;
        public int LastSaveAttempts;
        public long SuccessCount;
        public long FailedCount;
        public long StableCaptureFailedCount; // number of times stable copy failed since process start
        public long LastObservedHeadVersion;
        public long LastObservedBuffered;
    }

    /// <summary>Per-partition snapshot telemetry.</summary>
    /// <param name="PartitionKey">Logical partition identifier (index string).</param>
    /// <param name="LastVersion">Last successfully persisted snapshot version.</param>
    /// <param name="LastSnapshotAt">UTC timestamp when last snapshot completed.</param>
    /// <param name="EventsSinceLastSnapshot">Event version delta between last 2 successful snapshots.</param>
    /// <param name="LastSaveAttempts">Attempts taken for most recent successful (or failed) save.</param>
    /// <param name="SuccessCount">Total successful snapshot saves.</param>
    /// <param name="FailedCount">Total failed snapshot save jobs (after retries exhausted).</param>
    /// <param name="StableCaptureFailedCount">Times a stable capture attempt failed due to contention.</param>
    /// <param name="HeadVersion">Current live logical version (tail) at metrics snapshot time (approx).</param>
    /// <param name="CurrentBuffered">Approximate number of events currently buffered in the partition.</param>
    public readonly record struct PartitionSnapshotInfo(string PartitionKey, long LastVersion, DateTimeOffset LastSnapshotAt, long EventsSinceLastSnapshot, int LastSaveAttempts, long SuccessCount, long FailedCount, long StableCaptureFailedCount, long HeadVersion, long CurrentBuffered);
    /// <summary>Aggregated snapshotter metrics.</summary>
    /// <param name="Partitions">Per-partition metrics array.</param>
    /// <param name="DroppedJobs">Total snapshot jobs dropped due to queue backpressure.</param>
    /// <param name="TotalFailedJobs">Total jobs that reached max attempts and failed.</param>
    /// <param name="StableCaptureFailures">Total stable capture failures across all partitions.</param>
    public readonly record struct SnapshotterMetrics(PartitionSnapshotInfo[] Partitions, long DroppedJobs, long TotalFailedJobs, long StableCaptureFailures);

    /// <summary>Create a snapshotter supervising background save jobs.</summary>
    public Snapshotter(EventStore<Event> store, SnapshotOptions options, ISnapshotSerializer serializer, ISnapshotStore snapshotStore, ILogger? logger = null, IEventDeltaWriter? deltaWriter = null, IBackoffPolicy? backoff = null)
    {
        _store = store;
        Options = options;
        _serializer = serializer;
        _storeBackend = snapshotStore;
        _logger = logger;
        _backoff = backoff ?? new ExponentialBackoffPolicy(options.BackoffBaseDelay, options.BackoffFactor);
        _concurrency = new SemaphoreSlim(options.MaxConcurrentSnapshotJobs);
        _lastSnapshotTicks = Stopwatch.GetTimestamp();
        _activity = options.EnableLocalTracing ? new ActivitySource("LockFree.EventStore") : null;
        _ = deltaWriter; // placeholder until deltas implemented
    }

    /// <summary>Increment internal event counter when new events appended.</summary>
    public void NotifyAppended(int count)
    {
        if (!Options.Enabled)
        {
            return;
        }
        _ = Interlocked.Add(ref _eventsSinceLast, count);
    }

    /// <summary>Main background loop evaluating triggers and dispatching jobs.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        if (!Options.Enabled)
        {
            return;
        }
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var elapsed = (Stopwatch.GetTimestamp() - Volatile.Read(ref _lastSnapshotTicks)) / (double)Stopwatch.Frequency;
                var intervalReached = elapsed >= Options.Interval.TotalSeconds;
                var eventCount = Interlocked.Exchange(ref _eventsSinceLast, 0);
                var eventsReached = eventCount >= Options.MinEventsBetweenSnapshots;
                if (intervalReached || eventsReached)
                {
                    EnqueueAllPartitions();
                    _ = Interlocked.Exchange(ref _lastSnapshotTicks, Stopwatch.GetTimestamp());
                }
                else if (eventCount > 0)
                {
                    // Restore the count if we didn't trigger
                    _ = Interlocked.Add(ref _eventsSinceLast, eventCount);
                }
                await ProcessQueueAsync(ct).ConfigureAwait(false);
                await Task.Delay(250, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // normal
            }
            catch (Exception ex)
            {
                if (_logger != null)
                {
                    _logLoopError(_logger, ex);
                }
            }
        }
    }

    private void EnqueueAllPartitions()
    {
        // Enqueue every real partition (indices as string keys) for per-partition snapshots (RFC005)
        var partitions = _store.Partitions;
        for (var i = 0; i < partitions; i++)
        {
            TryEnqueueJob(i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    // Added for graceful shutdown final snapshot pass
    internal void ForceEnqueueAllPartitionsForShutdown()
    {
        EnqueueAllPartitions();
    }

    internal async Task DrainQueueForShutdownAsync(CancellationToken ct)
    {
        // Continuously process queue until empty and all running jobs finish.
        while (true)
        {
            await ProcessQueueAsync(ct).ConfigureAwait(false);

            if (!_jobQueue.IsEmpty)
            {
                // More items enqueued while processing; loop again.
                continue;
            }

            // Materialize task list safely
            var taskList = new List<Task>(_runningJobs.Count);
            foreach (var kvp in _runningJobs)
            {
                taskList.Add(kvp.Key);
            }
            if (taskList.Count == 0)
            {
                break; // nothing running
            }
            try
            {
                await Task.WhenAll(taskList).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Ignore individual job failures here; they were already logged in ExecuteJobAsync.
            }
            // Loop again in case new tasks slipped in between snapshot and await.
            if (_jobQueue.IsEmpty && _runningJobs.IsEmpty)
            {
                break;
            }
        }
    }

    private void TryEnqueueJob(string key)
    {
        if (_jobQueue.Count >= Options.MaxPendingSnapshotJobs)
        {
            _ = Interlocked.Increment(ref _droppedJobs);
            if (_logger != null)
            {
                _logJobDropped(_logger, Options.MaxPendingSnapshotJobs, null);
            }
            return;
        }
        _jobQueue.Enqueue(key);
    }

    private async Task ProcessQueueAsync(CancellationToken ct)
    {
        while (_jobQueue.TryDequeue(out var key))
        {
            await _concurrency.WaitAsync(ct).ConfigureAwait(false);
            Task? jobTask = null;
            jobTask = Task.Run(async () =>
            {
                try
                {
                    await ExecuteJobAsync(key, ct).ConfigureAwait(false);
                }
                finally
                {
                    _ = _concurrency.Release();
                    if (jobTask != null)
                    {
                        _ = _runningJobs.TryRemove(jobTask, out _); // discard bool result
                    }
                }
            }, ct);
            _ = _runningJobs.TryAdd(jobTask, 0); // discard bool result
        }
    }

    private async Task ExecuteJobAsync(string partitionKey, CancellationToken ct)
    {
        using var act = _activity?.StartActivity("snapshot.save");

        if (!TryCaptureStableState(partitionKey, act, out var state))
        {
            return; // stable capture failed; metrics & tracing already recorded
        }

        for (var attempt = 1; attempt <= Options.MaxSaveAttempts; attempt++)
        {
            try
            {
                await SaveSnapshotCoreAsync(partitionKey, state, attempt, act, ct).ConfigureAwait(false);
                return; // success
            }
            catch (Exception ex) when (attempt < Options.MaxSaveAttempts && !ct.IsCancellationRequested)
            {
                var delay = _backoff.NextDelay(attempt);
                if (_logger != null)
                {
                    _logSaveRetry(_logger, attempt, delay.TotalMilliseconds, ex);
                }
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                RecordFailure(partitionKey, attempt, ex, act);
                return;
            }
        }
    }

    private bool TryCaptureStableState(string partitionKey, Activity? act, out PartitionState state)
    {
        if (_store.TryGetStableView(partitionKey, out state))
        {
            return true;
        }
        RecordStableCaptureFailure(partitionKey, act);
        return false;
    }

    private void RecordStableCaptureFailure(string partitionKey, Activity? act)
    {
        var status = _partitionStatus.GetOrAdd(partitionKey, _ => new PartitionSnapshotInternal());
        lock (status)
        {
            status.StableCaptureFailedCount++;
        }
        _ = Interlocked.Increment(ref _stableCaptureFailures);
        if (_logger != null)
        {
            _logStableFail(_logger, partitionKey, null);
        }
        _ = act?.AddTag("partition", partitionKey);
        _ = act?.AddTag("outcome", "stable-capture-failed");
    }

    private async Task SaveSnapshotCoreAsync(string partitionKey, PartitionState state, int attempt, Activity? act, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        await _serializer.SerializeAsync(ms, state, ct).ConfigureAwait(false);
        ms.Position = 0;
        var meta = new SnapshotMetadata(partitionKey, state.Version, state.TakenAt, state.SchemaVersion);
        await _storeBackend.SaveAsync(meta, ms, ct).ConfigureAwait(false);
        _ = RunPruneAsync(partitionKey, ct); // fire & forget
        RecordSuccess(partitionKey, state, attempt, ms.Length, act);
    }

    private Task RunPruneAsync(string partitionKey, CancellationToken ct)
    {
        return Task.Run(async () =>
        {
            try
            {
                await _storeBackend.PruneAsync(partitionKey, Options.SnapshotsToKeep, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // expected on shutdown
            }
            catch (Exception ex)
            {
                if (_logger != null)
                {
                    _logPruneFailed(_logger, partitionKey, ex);
                }
            }
        }, ct);
    }

    private void RecordSuccess(string partitionKey, PartitionState state, int attempt, long bytes, Activity? act)
    {
        _ = act?.AddTag("partition", partitionKey);
        _ = act?.AddTag("version", state.Version);
        _ = act?.AddTag("bytes", bytes);
        _ = act?.AddTag("attempts", attempt);
        _ = act?.AddTag("outcome", "success");
        var status = _partitionStatus.GetOrAdd(partitionKey, _ => new PartitionSnapshotInternal());
        lock (status)
        {
            var prevVersion = status.LastVersion;
            var delta = prevVersion > 0 && state.Version >= prevVersion ? state.Version - prevVersion : 0;
            status.LastVersion = state.Version;
            status.LastSnapshotAt = state.TakenAt;
            status.EventsSinceLastSnapshot = delta;
            status.LastSaveAttempts = attempt;
            status.SuccessCount++;
        }
    }

    private void RecordFailure(string partitionKey, int attempt, Exception ex, Activity? act)
    {
        if (_logger != null)
        {
            _logSaveFailed(_logger, attempt, ex);
        }
        var status = _partitionStatus.GetOrAdd(partitionKey, _ => new PartitionSnapshotInternal());
        lock (status)
        {
            status.LastSaveAttempts = attempt;
            status.FailedCount++;
        }
        _ = Interlocked.Increment(ref _totalFailed);
        _ = act?.AddTag("partition", partitionKey);
        _ = act?.AddTag("attempts", attempt);
        _ = act?.AddTag("error", ex.GetType().Name);
        _ = act?.AddTag("outcome", "failure");
    }

    /// <summary>Returns current snapshotter metrics snapshot.</summary>
    public SnapshotterMetrics GetMetrics()
    {
        var list = new List<PartitionSnapshotInfo>(_partitionStatus.Count);
        foreach (var kvp in _partitionStatus)
        {
            var s = kvp.Value;
            long headVersion = 0;
            long buffered = 0;
            // Partition keys are numeric indices; sample live head & buffered via store internals.
            if (int.TryParse(kvp.Key,
                            System.Globalization.NumberStyles.None,
                             System.Globalization.CultureInfo.InvariantCulture,
                             out _) && _store.TryGetStableView(kvp.Key, out var state))
            {
                // Attempt a lightweight stable copy to read current version without materializing events when possible.
                headVersion = state.Version;
                buffered = state.Events.LongLength; // length of stable snapshot (<= capacity)
            }
            lock (s)
            {
                s.LastObservedHeadVersion = headVersion;
                s.LastObservedBuffered = buffered;
                list.Add(new PartitionSnapshotInfo(kvp.Key, s.LastVersion, s.LastSnapshotAt, s.EventsSinceLastSnapshot, s.LastSaveAttempts, s.SuccessCount, s.FailedCount, s.StableCaptureFailedCount, s.LastObservedHeadVersion, s.LastObservedBuffered));
            }
        }
        var array = list.ToArray();
        return new SnapshotterMetrics(array, Interlocked.Read(ref _droppedJobs), Interlocked.Read(ref _totalFailed), Interlocked.Read(ref _stableCaptureFailures));
    }

    /// <summary>Restores in-memory partitions from latest persisted snapshots (best-effort).</summary>
    public async Task<int> RestoreFromSnapshotsAsync(CancellationToken ct = default)
    {
        if (!Options.Enabled)
        {
            return 0; // feature off, nothing to restore
        }
        var restored = 0;
        await foreach (var key in _storeBackend.ListPartitionKeysAsync(ct).ConfigureAwait(false))
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }
            var result = await LoadValidStateAsync(key, ct).ConfigureAwait(false);
            if (result is null)
            {
                continue; // skipped (missing / corrupted / schema ignored / invalid key)
            }
            var (state, index) = result.Value;
            if (_store.TryRestorePartition(index, state.Events))
            {
                restored++;
            }
        }
        return restored;
    }

    private async ValueTask<(PartitionState State, int Index)?> LoadValidStateAsync(string key, CancellationToken ct)
    {
        var latest = await _storeBackend.TryLoadLatestAsync(key, ct).ConfigureAwait(false);
        if (latest is null)
        {
            return null; // no snapshot
        }
        await using var data = latest.Value.Data;
        PartitionState state;
        try
        {
            state = await _serializer.DeserializeAsync(data, ct).ConfigureAwait(false);
        }
        catch
        {
            return null; // corrupted snapshot
        }
        // Schema enforcement
        if (Options.ExpectedSchemaVersion.HasValue)
        {
            if (state.SchemaVersion != Options.ExpectedSchemaVersion.Value)
            {
                throw new InvalidOperationException($"Snapshot schema mismatch. Expected={Options.ExpectedSchemaVersion.Value} Actual={state.SchemaVersion} Partition={state.PartitionKey}");
            }
        }
        else if (state.SchemaVersion != 1)
        {
            return null; // ignore non-1 when not enforcing
        }
        // Partition key must be integer index
        return !int.TryParse(state.PartitionKey, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var p)
            ? null
            : (state, p);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _concurrency.Dispose();
    }

    internal SnapshotOptions Options { get; }
}

/// <summary>
/// Hosted service wrapper for Snapshotter.
/// </summary>
public sealed class SnapshotHostedService(Snapshotter snapshotter) : BackgroundService
{
    private readonly Snapshotter _snapshotter = snapshotter;
    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return _snapshotter.RunAsync(stoppingToken);
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Attempt graceful final snapshot pass if configured
        var opts = _snapshotter.Options;
        if (opts.FinalSnapshotOnShutdown && opts.Enabled)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(opts.FinalSnapshotTimeout);
            try
            {
                // Trigger enqueue directly and drain
                _snapshotter.ForceEnqueueAllPartitionsForShutdown();
                await _snapshotter.DrainQueueForShutdownAsync(cts.Token).ConfigureAwait(false);
            }
            catch
            {
                // swallow errors during shutdown finalization
            }
        }
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}

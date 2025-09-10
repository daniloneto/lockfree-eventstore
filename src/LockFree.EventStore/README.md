# LockFree.EventStore
[![CI](https://github.com/daniloneto/lockfree-eventstore/actions/workflows/ci.yml/badge.svg)](https://github.com/daniloneto/lockfree-eventstore/actions)
[![NuGet](https://img.shields.io/nuget/v/LockFree.EventStore.svg)](https://www.nuget.org/packages/LockFree.EventStore)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=daniloneto_lockfree-eventstore&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=daniloneto_lockfree-eventstore)

**An in-memory event store, running as a service, to synchronize and validate operations across multiple instances with high concurrency and no locks.**

---
## 🚀 Get started in 3 steps

### 1. Run the server
```bash
docker run --rm -p 7070:7070 daniloneto/lockfree-eventstore:latest
```

### 2. Add the client
```bash
dotnet add package LockFree.EventStore
```

### 3. Write and read
```csharp
var es = new EventStoreClient("http://localhost:7070");
await es.Append("gateway/orders", new OrderCreated { Id = "o-1", Valor = 123 });
await foreach (var ev in es.Read("gateway/orders", from: 0))
{
    /* handle event */
}
```

### 🔁 Client Sample
See `samples/ClientSample` for an example that:
- Sends events in parallel to `gateway/orders`
- Reads the events back
- Computes local aggregations

To run:
```bash
docker run --rm -p 7070:7070 daniloneto/lockfree-eventstore:latest
cd samples/ClientSample
 dotnet run
```

## 🌐 Example with multiple Gateways (docker-compose)

Start 1 EventStore, 3 gateways and Nginx load balancing:
```bash
docker compose up --build
```
Test sending orders (load balanced across gateways):
```bash
curl -X POST http://localhost:8080/orders
curl -X POST 'http://localhost:8080/orders/bulk?n=50'
```
View statistics:
```bash
curl http://localhost:8080/stats/local    # stats of one gateway (one of the 3)
curl http://localhost:8080/stats/global   # global consolidation (via central reader)
```
---

## 💡 Why use it
- **Real concurrency:** multiple writers without mutexes.
- **Guaranteed integrity:** consistent ordering, conditional append, and idempotency.
- **Simple operation:** no external coordination, no dependencies.

---

## 📌 Typical scenario
Two (or more) gateways behind a load balancer need to record operations to the same stream.  
**Lockfree.EventStore** ensures order and integrity even under high parallelism, without relying on locks, keeping all state in memory.

---

## 📚 Complete documentation

Below is the full technical documentation of the API, advanced features, benchmarks, and usage examples.

## Key Features
- Lock-free MPMC writes with FIFO discard
- Key-based partitioning for high concurrency
- Consistent snapshots without blocking producers
- Functional aggregations and time-window queries
- Zero external dependencies, ready for AOT/Trimming
- Fluent API for advanced configuration
- Built-in metrics and observability
- Specialized aggregations (Sum, Average, Min, Max)

## Basic Usage Example
```csharp
var store = new EventStore<Pedido>();
store.TryAppend(new Pedido { Id = 1, Valor = 10m, Timestamp = DateTime.UtcNow });

var total = store.Aggregate(() => 0m, (acc, e) => acc + e.Valor,
    from: DateTime.UtcNow.AddMinutes(-10));
```

## New Constructors
```csharp
// Explicit capacity
var store = new EventStore<Pedido>(capacity: 100_000);

// Capacity and partitions
var store = new EventStore<Pedido>(capacity: 50_000, partitions: 8);

// Advanced configuration
var store = new EventStore<Pedido>(new EventStoreOptions<Pedido>
{
    Capacity = 100_000,
    Partitions = 16,
    OnEventDiscarded = evt => Logger.LogTrace("Event discarded: {Event}", evt),
    OnCapacityReached = () => Metrics.IncrementCounter("eventstore.capacity_reached"),
    TimestampSelector = new PedidoTimestampSelector(),
    // RFC 002: disable window tracking for pure append/snapshot workloads
    EnableWindowTracking = false
});

// Fluent API
var store = new EventStoreBuilder<Pedido>()
    .WithCapacity(100_000)
    .WithPartitions(8)
    .OnDiscarded(evt => Log(evt))
    .OnCapacityReached(() => NotifyAdmin())
    .WithTimestampSelector(new PedidoTimestampSelector())
    // RFC 002: opt-out when window queries are not used
    .WithEnableWindowTracking(false)
    .Create();
```

## State Properties
```csharp
store.Count          // Current number of events
store.Capacity       // Maximum configured capacity
store.IsEmpty        // Whether it's empty
store.IsFull         // Whether it reached maximum capacity
store.Partitions     // Number of partitions
```

## Specialized Aggregations
```csharp
// Count by time window
var count = store.Count(from: inicio, to: fim);

// Sum of values
var sum = store.Sum(evt => evt.Valor, from: inicio, to: fim);

// Average
var avg = store.Average(evt => evt.Valor, from: inicio, to: fim);

// Min and max
var min = store.Min(evt => evt.Pontuacao, from: inicio, to: fim);
var max = store.Max(evt => evt.Pontuacao, from: inicio, to: fim);

// With filters
var filteredSum = store.Sum(
    evt => evt.Valor, 
    filter: evt => evt.Tipo == "Pagamento",
    from: inicio, 
    to: fim
);
```

Note: Time-filtered queries require `EnableWindowTracking = true`. When disabled, a clear InvalidOperationException is thrown: "Window tracking is disabled. EnableWindowTracking must be true to use window queries."

## Snapshots
`Snapshot()` returns an approximate immutable copy of the current state of all partitions, ordered from the oldest to the newest event per partition.

### Persistent Snapshots (RFC-005)
Adds an optional persistence layer that periodically saves partition states to disk without impacting the hot append/query path.

Key goals:
- Lock-free stable partition view creation (no global locks, producer never blocked)
- Atomic filesystem writes (temp file + rename) to avoid partial/corrupted snapshots
- Fast startup via `RestoreFromSnapshotsAsync()` (warm memory reconstruction)
- Bounded retry with exponential backoff for transient I/O failures
- Pruning of older snapshots keeping only the most recent N
- Fail-fast validation of snapshot configuration
- Optional lightweight local tracing (disabled by default)
- Hooks prepared for future delta replay (not implemented yet)

#### Configuration
```csharp
var store = new EventStore<Event>(new EventStoreOptions<Event>
{
    // existing core options
    CapacityPerPartition = 1_000_000,
    Partitions = Environment.ProcessorCount,
    EnableFalseSharingProtection = true
});

// Configure snapshot subsystem (one-time)
var snapshotter = store.ConfigureSnapshots(
    new SnapshotOptions
    {
        Enabled = true,
        Interval = TimeSpan.FromMinutes(2),            // Time-based trigger (can be combined with MinEventsBetweenSnapshots)
        MinEventsBetweenSnapshots = 50_000,            // Event-count trigger
        MaxConcurrentSnapshotJobs = 2,                 // Limit concurrent save jobs
        SnapshotsToKeep = 3,                           // Pruning window
        MaxSaveAttempts = 5,                           // Retry attempts for transient errors
        BackoffBaseDelay = TimeSpan.FromMilliseconds(100),
        BackoffFactor = 2.0,
        CompactBeforeSnapshot = true,                  // (Future compaction hook)
        EnableLocalTracing = false                     // Enables ActivitySource if true
    },
    serializer: new BinarySnapshotSerializer(),
    store: new FileSystemSnapshotStore("snapshots")
);

await store.RestoreFromSnapshotsAsync(); // Rebuild in-memory state before serving traffic
```

#### Stable View Extraction
Internally `TryGetStableView(partitionKey, out PartitionState state)` performs a double-read of head counters (HeadVersion → HeadIndex → HeadVersion) with bounded retries to obtain a coherent cut, copying the ring into a contiguous buffer (handling wrap-around with at most two spans) without allocating per event.

#### Atomic Save Protocol
1. Serialize to a temporary file: `<partition>_<version>_<ticks>.snap.tmp`
2. Flush & (where available) use WriteThrough / Flush(true)
3. Atomic rename to final: `.snap` (same volume) via `File.Move(temp, final, overwrite:false)`. A collision (target already exists) is treated as a logic/corruption anomaly and the move throws (fail-fast) instead of silently overwriting an existing snapshot.
4. Prune old snapshots asynchronously (best-effort; errors do not affect hot path)

Temporary `.tmp` files are ignored during load. Stray/unknown non-`.snap` files are also skipped both on restore and pruning to avoid interfering with normal operation.

#### Retry & Backoff
Transient I/O failures trigger exponential backoff:
```
nextDelay = baseDelay * factor^(attempt-1) + small jitter
```
Stops after `MaxSaveAttempts`. Failures are logged; producers remain unaffected.

#### Pruning
After a successful save, the N newest snapshots (by version/timestamp) are retained; older ones are deleted. Failures during pruning are logged and skipped.

#### Restore
`RestoreFromSnapshotsAsync()` enumerates partitions via the snapshot store, loads the most recent `.snap` for each, validates schema version, and reconstructs the in-memory ring state. Delta replay hooks exist but are inactive.

#### Tracing & Metrics
- Optional `ActivitySource` emits: `snapshot.save` (partition, version, bytes, attempts, success) and `snapshot.prune` (kept, deleted)
- Internal counters track: head version per partition, events since last snapshot, last snapshot timestamp, failed attempts

#### Performance Impact
Measured p50 / p99 append latency regression ≤ +2% with snapshots enabled under benchmark load, staying within target budget. The stable view logic uses only volatile reads and bounded retries; no global locks are introduced.

#### Limitations & Notes
- Not a durability guarantee per individual event
- Delta / incremental replay not yet implemented (future extension)
- Tracing disabled by default to avoid extra allocations
- Requires explicit enabling (`SnapshotOptions.Enabled = true`)

### Snapshot Samples
Two focused samples demonstrate the snapshot subsystem:

#### 1. SnapshotSensors (Console)
High-frequency synthetic sensor workload (temperature + humidity) showing:
- Warm start (restores ring buffers from latest snapshots on boot)
- Periodic snapshots (time + event count triggers)
- Graceful shutdown producing a final snapshot
- Atomic write path (temporary .tmp then rename to .snap)
- Pruning (keeps only the last N versions)
- Snapshot + store metrics printed every 10s

Run:
```bash
dotnet run --project samples/SnapshotSensors/SnapshotSensors.csproj
```
Stop (Ctrl+C) and run again; look for:
```
[BOOT] Partitions restauradas de snapshot: X
```
If X > 0 the state was warm-started.

Key configuration (Program.cs):
- Interval = 5s
- MinEventsBetweenSnapshots = 100_000
- SnapshotsToKeep = 3
- FinalSnapshotOnShutdown = true (timeout 3s)
- Compression enabled (`BinarySnapshotSerializer(compress: true)`)

#### 2. SnapshotSensorsApi (Minimal API)
HTTP API receiving JSON sensor readings:
- POST /sensor (deviceId, temperature, humidity, timestampUtc?) → appends two events (temp/humidity) distributed across partitions
- GET /state → aggregated min/max/avg/count per metric + approximate totals
- GET /metrics → internal event store + snapshot subsystem metrics
- Warm start using `RestoreFromSnapshotsAsync()` before serving requests
- Background snapshotter + periodic metrics printing

Run:
```bash
dotnet run --project samples/SnapshotSensorsApi/SnapshotSensorsApi.csproj
```
Send readings:
```bash
curl -X POST http://localhost:5000/sensor \
  -H "Content-Type: application/json" \
  -d '{"deviceId":"dev-1","temperature":22.5,"humidity":48.2}'
```
Inspect state/metrics:
```bash
curl http://localhost:5000/state
curl http://localhost:5000/metrics
```

Snapshot config (Program.cs):
- Interval = 10s
- MinEventsBetweenSnapshots = 50_000
- MaxConcurrentSnapshotJobs = max(2, partitions/4)
- SnapshotsToKeep = 3
- FinalSnapshotOnShutdown = true (timeout 5s)
- Compression enabled

Both samples showcase that snapshot persistence does not block high-frequency appends and that partial files are never observed (atomic rename). Adjust `Interval`, `MinEventsBetweenSnapshots`, or enable directory fsync (Unix) to explore durability vs performance.

## Cleanup and Maintenance
```csharp
// Clear all events
store.Clear();
store.Reset(); // Alias for Clear()

// Purge old events (requires TimestampSelector)
store.Purge(olderThan: DateTime.UtcNow.AddHours(-1));
```

## Metrics and Observability
```csharp
// Detailed statistics
store.Statistics.TotalAppended        // Total appended events
store.Statistics.TotalDiscarded       // Total discarded events
store.Statistics.AppendsPerSecond     // Current append rate
store.Statistics.LastAppendTime       // Timestamp of the last append
```

## Examples

### MetricsDashboard
Fully featured web API for collecting and querying real-time metrics:

```bash
cd .\samples\MetricsDashboard
 dotnet run
```

Available endpoints:
- `POST /metrics` - Add metric
- `GET /metrics/sum?label=cpu_usage` - Sum values by label
- `GET /metrics/top?k=5` - Top K metrics

See `samples/MetricsDashboard/TESTING.md` for a complete testing guide.

## Full API
- `TryAppend(event)` — Adds an event, lock-free
- `Aggregate` — Aggregates values by time window
- `Snapshot()` — Returns an immutable copy of events
- `Count/Sum/Average/Min/Max` — Specialized aggregations
- `Clear/Reset/Purge` — Cleanup methods
- `Query` — Flexible queries with filters
- `Statistics` — Monitoring metrics

## Partitions
The default number of partitions is `Environment.ProcessorCount`. You can force the partition using `TryAppend(e, partition)`.

## Snapshots
`Snapshot()` returns an approximate immutable copy of the current state of all partitions, ordered from the oldest to the newest event per partition.

## Performance
Designed for high concurrency and low latency. Global order across partitions is approximate.

---

## Performance Benchmarks

### Value Types vs Reference Types

| Operation                 | Value Type    | Reference Type  | Improvement |
|--------------------------|---------------|-----------------|-------------|
| Event append             | 560 ms        | 797 ms          | 42% faster  |
| Event iteration          | 35.8 ns       | 132.5 ns        | 74% faster  |
| Event queries            | 393.5 ns      | 1,749.1 ns      | 77% faster  |

### Structure of Arrays (SoA) vs Array of Structures (AoS)

| Operation                 | SoA           | AoS             | Improvement |
|--------------------------|---------------|-----------------|-------------|
| Key aggregation          | 55.2 ms       | 74.6 ms         | 26% faster  |
| Memory usage             | Lower         | Higher          | Varies      |

**Conclusions:**
1. Value types are significantly faster than reference types for reads and writes.
2. SoA improves cache locality and reduces memory pressure.

## Limitations
- Global order is only approximate across partitions
- Fixed capacity; old events are discarded when exceeded

## License
MIT

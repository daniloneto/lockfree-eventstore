# LockFree.EventStore
[![CI](https://github.com/daniloneto/lockfree-eventstore/actions/workflows/ci.yml/badge.svg)](https://github.com/daniloneto/lockfree-eventstore/actions)
[![NuGet](https://img.shields.io/nuget/v/LockFree.EventStore.svg)](https://www.nuget.org/packages/LockFree.Event.Store)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=daniloneto_lockfree-eventstore&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=daniloneto_lockfree-eventstore)
[![SonarQube Cloud](https://sonarcloud.io/images/project_badges/sonarcloud-light.svg)](https://sonarcloud.io/summary/new_code?id=daniloneto_lockfree-eventstore)
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
var store = new EventStore<Pedido>(capacidade: 100_000);

// Capacity and partitions
var store = new EventStore<Pedido>(capacidade: 50_000, particoes: 8);

// Advanced configuration
var store = new EventStore<Pedido>(new EventStoreOptions<Pedido>
{
    Capacidade = 100_000,
    Particoes = 16,
    OnEventDiscarded = evt => Logger.LogTrace("Event discarded: {Event}", evt),
    OnCapacityReached = () => Metrics.IncrementCounter("eventstore.capacity_reached"),
    TimestampSelector = new PedidoTimestampSelector()
});

// Fluent API
var store = EventStore.For<Pedido>()
    .WithCapacity(100_000)
    .WithPartitions(8)
    .OnDiscarded(evt => Log(evt))
    .OnCapacityReached(() => NotifyAdmin())
    .WithTimestampSelector(new PedidoTimestampSelector())
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

## Snapshots with Filters
```csharp
// Filtered snapshot
var eventosRecentes = store.Snapshot(
    filter: evt => evt.Timestamp > DateTime.UtcNow.AddMinutes(-5)
);

// Snapshot by time window
var snapshot = store.Snapshot(from: inicio, to: fim);

// Snapshot with filter and time window
var filtrado = store.Snapshot(
    filter: evt => evt.Valor > 100,
    from: inicio,
    to: fim
);
```

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
3. For high throughput, the `EventStoreV2` implementation is recommended.

```csharp
// Using EventStoreV2 with value types
var store = new EventStoreV2(capacidade: 1_000_000, particoes: 16);
store.Add("sensor1", 25.5, DateTime.UtcNow.Ticks);
double media = store.Average("sensor1");
```

## Limitations
- Global order is only approximate across partitions
- Fixed capacity; old events are discarded when exceeded

## License
MIT

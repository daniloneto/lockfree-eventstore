using System;
using Xunit;

namespace LockFree.EventStore.Tests;

public class AggregationTests
{
    [Fact]
    public void Aggregate_Sums_Within_Window()
    {
        var options = new EventStoreOptions<Order> { TimestampSelector = new OrderTimestampSelector(), CapacityPerPartition = 50, Partitions = 1 };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-1)));
        var total = store.Aggregate(() => 0m, (acc, o) => acc + o.Amount, from: now.AddMinutes(-2));
        Assert.Equal(20m, total);
    }

    [Fact]
    public void AggregateBy_Groups_By_Label()
    {
        var options = new EventStoreOptions<MetricEvent> { TimestampSelector = new MetricTimestampSelector(), CapacityPerPartition = 50, Partitions = 2 };
        var store = new EventStore<MetricEvent>(options);
        var now = DateTime.UtcNow;
        store.TryAppend(new MetricEvent("cpu", 0.5, now));
        store.TryAppend(new MetricEvent("cpu", 0.7, now));
        store.TryAppend(new MetricEvent("mem", 1.0, now));
        var result = store.AggregateBy(m => m.Label, () => 0d, (acc, m) => acc + m.Value);
        Assert.Equal(1.2, result["cpu"], 1);
        Assert.Equal(1.0, result["mem"], 1);
    }
}

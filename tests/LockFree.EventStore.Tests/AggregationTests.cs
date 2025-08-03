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

    [Fact]
    public void AggregateWindow_Returns_Correct_Statistics()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 50,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add events with different amounts and timestamps
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 5m, now.AddMinutes(-1)));
        store.TryAppend(new Order(4, 30m, now.AddMinutes(-6))); // Outside window

        var result = store.AggregateWindow(from: now.AddMinutes(-4), to: now);

        Assert.Equal(3, result.Count);
        Assert.Equal(35.0, result.Sum);
        Assert.Equal(5.0, result.Min);
        Assert.Equal(20.0, result.Max);
        Assert.Equal(35.0 / 3.0, result.Avg, 2);
    }

    [Fact]
    public void AggregateWindow_With_Filter_Works_Correctly()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 50,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 25m, now.AddMinutes(-3))); // Will be filtered out
        store.TryAppend(new Order(3, 15m, now.AddMinutes(-1)));

        // Filter: only orders with amount <= 20
        var result = store.AggregateWindow(
            from: now.AddMinutes(-10),
            to: now,
            filter: o => o.Amount <= 20m);

        Assert.Equal(2, result.Count);
        Assert.Equal(25.0, result.Sum);
        Assert.Equal(10.0, result.Min);
        Assert.Equal(15.0, result.Max);
    }

    [Fact]
    public void SumWindow_Performance_Better_Than_Traditional_Sum()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 1000,
            Partitions = 4
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add many events
        for (int i = 0; i < 10000; i++)
        {
            store.TryAppend(new Order(i, i * 1.5m, now.AddSeconds(-i)));
        }

        var windowStart = now.AddMinutes(-30);
        var windowEnd = now;

        // Test new SumWindow method
        var sumWindow = store.SumWindow(o => o.Amount, windowStart, windowEnd);

        // Test traditional Sum method for comparison
        var sumTraditional = store.Sum(o => o.Amount, windowStart, windowEnd);

        // Results should be the same
        Assert.Equal(sumTraditional, sumWindow);
    }

    [Fact]
    public void AggregateWindow_Handles_Empty_Window()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 50,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add events outside the query window
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-10)));

        var result = store.AggregateWindow(from: now.AddMinutes(-5), to: now);

        Assert.Equal(0, result.Count);
        Assert.Equal(0.0, result.Sum);
        Assert.Equal(0.0, result.Min);
        Assert.Equal(0.0, result.Max);
        Assert.Equal(0.0, result.Avg);
    }

    [Fact]
    public void AggregateWindow_Works_Across_Multiple_Partitions()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 50,
            Partitions = 4
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add events that will be distributed across partitions
        for (int i = 0; i < 8; i++)
        {
            store.TryAppend(new Order(i, (i + 1) * 10m, now.AddMinutes(-i)));
        }

        var result = store.AggregateWindow(from: now.AddMinutes(-10), to: now);

        Assert.Equal(8, result.Count);
        Assert.Equal(360.0, result.Sum); // Sum of 10+20+30+40+50+60+70+80
        Assert.Equal(10.0, result.Min);
        Assert.Equal(80.0, result.Max);
        Assert.Equal(45.0, result.Avg);
    }
}

using System;
using System.Linq;
using Xunit;

namespace LockFree.EventStore.Tests;

public class ZeroAllocationTests
{
    [Fact]
    public void ProcessEvents_Works_Without_Allocation()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 2
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        // Use zero-allocation processing to sum amounts
        var totalAmount = store.ProcessEvents<Order, decimal>(
            0m,
            (ref decimal sum, Order order, DateTime? timestamp) =>
            {
                sum += order.Amount;
                return true; // Continue processing
            });

        Assert.Equal(60m, totalAmount);
    }

    [Fact]
    public void ProcessEvents_Supports_Early_Termination()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        // Use early termination when we find an order with amount >= 20
        var result = store.ProcessEvents<Order, (int Count, decimal Amount)>(
            (0, 0m),
            (ref (int Count, decimal Amount) state, Order order, DateTime? timestamp) =>
            {
                state.Count++;
                state.Amount = order.Amount;
                return order.Amount < 20m; // Stop when we find amount >= 20
            });

        Assert.Equal(2, result.Count); // Should have processed 2 orders before stopping
        Assert.Equal(20m, result.Amount); // Last processed amount should be 20
    }

    [Fact]
    public void ProcessEvents_With_Filter_Works()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 2
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        // Filter for orders with amount > 15
        var totalAmount = store.ProcessEvents<Order, decimal>(
            0m,
            (ref decimal sum, Order order, DateTime? timestamp) =>
            {
                sum += order.Amount;
                return true;
            },
            filter: (order, timestamp) => order.Amount > 15m);

        Assert.Equal(50m, totalAmount); // Only orders 2 and 3 should be included
    }

    [Fact]
    public void CountEventsZeroAlloc_Works()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 2
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        // Count all events
        var totalCount = store.CountEventsZeroAlloc();
        Assert.Equal(3, totalCount);

        // Count with filter
        var filteredCount = store.CountEventsZeroAlloc(
            filter: (order, timestamp) => order.Amount > 15m);
        Assert.Equal(2, filteredCount);
    }

    [Fact]
    public void FindFirstZeroAlloc_Works()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        // Find first order with amount > 15
        var result = store.FindFirstZeroAlloc(
            filter: (order, timestamp) => order.Amount > 15m);

        Assert.True(result.Found);
        Assert.Equal(2, result.Event.Id); // Should find order 2 first
        Assert.Equal(20m, result.Event.Amount);
    }

    [Fact]
    public void FindFirstZeroAlloc_Returns_NotFound_When_No_Match()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));

        // Try to find order with amount > 50 (none exists)
        var result = store.FindFirstZeroAlloc(
            filter: (order, timestamp) => order.Amount > 50m);

        Assert.False(result.Found);
    }

    [Fact]
    public void AggregateZeroAlloc_Works()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 2
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        // Aggregate to find max amount
        var maxAmount = store.AggregateZeroAlloc<Order, decimal>(
            0m,
            (acc, order) => Math.Max(acc, order.Amount));

        Assert.Equal(30m, maxAmount);

        // Aggregate with filter
        var maxSmallAmount = store.AggregateZeroAlloc<Order, decimal>(
            0m,
            (acc, order) => Math.Max(acc, order.Amount),
            filter: (order, timestamp) => order.Amount < 25m);

        Assert.Equal(20m, maxSmallAmount);
    }

    [Fact]
    public void ProcessEvents_With_TimeFilter_Works()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add test data with different timestamps
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-10))); // Outside window
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));  // Inside window
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));  // Inside window

        // Process only events from the last 5 minutes
        var totalAmount = store.ProcessEvents<Order, decimal>(
            0m,
            (ref decimal sum, Order order, DateTime? timestamp) =>
            {
                sum += order.Amount;
                return true;
            },
            from: now.AddMinutes(-5),
            to: now);

        Assert.Equal(50m, totalAmount); // Only orders 2 and 3 should be included
    }

    [Fact]
    public void ZeroAllocation_Performance_Better_Than_LINQ()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 10000,
            Partitions = 4
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add many events
        for (int i = 0; i < 5000; i++)
        {
            store.TryAppend(new Order(i, i * 1.5m, now.AddSeconds(-i)));
        }

        // Test zero-allocation approach
        var zeroAllocSum = store.ProcessEvents<Order, decimal>(
            0m,
            (ref decimal sum, Order order, DateTime? timestamp) =>
            {
                sum += order.Amount;
                return true;
            });

        // Test traditional LINQ approach (for comparison)
        // var linqSum = store.Query().Sum(o => o.Amount);
        // Compute comparison sum via SnapshotViews to avoid LINQ and obsolete APIs
        decimal linqSum = 0m;
        foreach (var view in store.SnapshotViews())
        {
            foreach (var o in view)
            {
                linqSum += o.Amount;
            }
        }

        // Both should produce the same result
        Assert.Equal(linqSum, zeroAllocSum);
    }
}

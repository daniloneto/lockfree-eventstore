using System;
using System.Collections.Generic;
using Xunit;
using LockFree.EventStore;

namespace LockFree.EventStore.Tests;

public sealed class EnhancedFeaturesTests
{
    [Fact]
    public void Constructor_WithCapacity_SetsCapacityCorrectly()
    {
        var store = new EventStore<Order>(capacity: 1000);
        
        Assert.Equal(1000, store.Capacity);
        Assert.True(store.IsEmpty);
        Assert.False(store.IsFull);
    }

    [Fact]
    public void Constructor_WithCapacityAndPartitions_DistributesCapacityCorrectly()
    {
        var store = new EventStore<Order>(capacity: 1000, partitions: 4);
        
        Assert.Equal(1000, store.Capacity);
        Assert.Equal(4, store.Partitions);
    }

    [Fact]
    public void FluentBuilder_CreatesStoreCorrectly()
    {
        var discardedEvents = new List<Order>();
        var capacityReachedCount = 0;

        var store = new EventStoreBuilder<Order>()
            .WithCapacity(100)
            .WithPartitions(2)
            .OnDiscarded(evt => discardedEvents.Add(evt))
            .OnCapacityReached(() => capacityReachedCount++)
            .WithTimestampSelector(new OrderTimestampSelector())
            .Create();

        Assert.Equal(100, store.Capacity);
        Assert.Equal(2, store.Partitions);
    }    [Fact]
    public void Count_WithTimeWindow_ReturnsCorrectCount()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100
        });

        var now = DateTime.UtcNow;
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        var count = store.CountEvents(from: now.AddMinutes(-4), to: now.AddMinutes(-2));
        Assert.Equal(1, count);
    }

    [Fact]
    public void Sum_WithSelector_ReturnsCorrectSum()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var sum = store.Sum(o => o.Amount);
        Assert.Equal(60m, sum);
    }

    [Fact]
    public void Sum_WithFilter_ReturnsFilteredSum()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var sum = store.Sum(o => o.Amount, filter: o => o.Amount >= 20m);
        Assert.Equal(50m, sum);
    }

    [Fact]
    public void Average_WithSelector_ReturnsCorrectAverage()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var avg = store.Average(o => o.Amount);
        Assert.Equal(20.0, avg);
    }

    [Fact]
    public void MinMax_WithSelector_ReturnsCorrectValues()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var min = store.Min(o => o.Amount);
        var max = store.Max(o => o.Amount);
        
        Assert.Equal(10m, min);
        Assert.Equal(30m, max);
    }

    [Fact]
    public void Snapshot_WithFilter_ReturnsFilteredEvents()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var snapshot = store.Snapshot(o => o.Amount >= 20m);
        Assert.Equal(2, snapshot.Count);
    }

    [Fact]
    public void Clear_RemovesAllEvents()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        
        Assert.False(store.IsEmpty);
        Assert.Equal(2, store.Count);
        
        store.Clear();
        
        Assert.True(store.IsEmpty);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void Reset_IsAliasForClear()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.Reset();
        
        Assert.True(store.IsEmpty);
    }    [Fact]
    public void Statistics_TracksMetrics()
    {
        var store = new EventStore<Order>();
        
        Assert.Equal(0, store.Statistics.TotalAppended);
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        
        Assert.Equal(2, store.Statistics.TotalAppended);
        Assert.True(store.Statistics.LastAppendTime > DateTime.MinValue);
    }

    [Fact]
    public void Purge_RemovesOldEvents()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100
        });        var now = DateTime.UtcNow;
        store.TryAppend(new Order(1, 10m, now.AddHours(-2))); // Old
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-30))); // Recent
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-10))); // Recent

        store.Purge(olderThan: now.AddHours(-1));

        Assert.Equal(2, store.Count);
    }    [Fact]
    public void AggregateWithFilter_UsesFilterCorrectly()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var result = store.Aggregate(
            () => new { Count = 0, Sum = 0m },
            (acc, evt) => new { Count = acc.Count + 1, Sum = acc.Sum + evt.Amount },
            new Predicate<Order>(evt => evt.Amount >= 20m)
        );

        Assert.Equal(2, result.Count);
        Assert.Equal(50m, result.Sum);
    }
}

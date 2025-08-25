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
    }

    [Fact]
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

        var count = store.CountEventsZeroAlloc(from: now.AddMinutes(-4), to: now.AddMinutes(-2));
        Assert.Equal(1, count);
    }

    [Fact]
    public void Sum_WithSelector_ReturnsCorrectSum()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var sum = store.SumZeroAlloc(o => (double)o.Amount);
        Assert.Equal(60.0, sum);
    }

    [Fact]
    public void Sum_WithFilter_ReturnsFilteredSum()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var sum = store.SumZeroAlloc(o => (double)o.Amount, filter: (e, _) => e.Amount >= 20m);
        Assert.Equal(50.0, sum);
    }

    [Fact]
    public void Average_WithSelector_ReturnsCorrectAverage()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var avg = store.AverageZeroAlloc(o => (double)o.Amount);
        Assert.Equal(20.0, avg);
    }

    [Fact]
    public void MinMax_WithSelector_ReturnsCorrectValues()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var min = store.MinZeroAlloc(o => (double)o.Amount);
        var max = store.MaxZeroAlloc(o => (double)o.Amount);
        
        Assert.Equal(10.0, min);
        Assert.Equal(30.0, max);
    }

    [Fact]
    public void Snapshot_WithFilter_ReturnsFilteredEvents()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var snapshot = store.Snapshot(e => e.Amount >= 20m);
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
    }

    [Fact]
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
        });

        var now = DateTime.UtcNow;
        store.TryAppend(new Order(1, 10m, now.AddHours(-2))); // Old
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-30))); // Recent
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-10))); // Recent

        store.Purge(olderThan: now.AddHours(-1));

        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void AggregateWithFilter_UsesFilterCorrectly()
    {
        var store = new EventStore<Order>();
        
        store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        store.TryAppend(new Order(3, 30m, DateTime.UtcNow));

        var result = store.AggregateZeroAlloc(
            new { Count = 0, Sum = 0m },
            (acc, evt) => evt.Amount >= 20m ? new { Count = acc.Count + 1, Sum = acc.Sum + evt.Amount } : acc
        );

        Assert.Equal(2, result.Count);
        Assert.Equal(50m, result.Sum);
    }
}

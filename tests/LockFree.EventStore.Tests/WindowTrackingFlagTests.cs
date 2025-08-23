using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class WindowTrackingFlagTests
{
    private static EventStore<Order> CreateStore(bool enableTracking)
    {
        var options = new EventStoreOptions<Order>
        {
            CapacityPerPartition = 1024,
            Partitions = 2,
            TimestampSelector = new OrderTimestampSelector(),
            ValueSelector = e => (double)e.Amount,
            WindowSizeTicks = TimeSpan.FromSeconds(1).Ticks,
            EnableWindowTracking = enableTracking
        };
        return new EventStore<Order>(options);
    }

    [Fact]
    public void Append_WithTrackingDisabled_PersistsButWindowQueriesThrow()
    {
        var store = CreateStore(enableTracking: false);
        var now = DateTime.UtcNow;
        // Append a few events
        for (int i = 0; i < 10; i++)
        {
            var e = new Order(i, i + 1, now.AddMilliseconds(i * 10));
            Assert.True(store.TryAppend("k", e, e.Timestamp.Ticks));
        }

        Assert.True(store.CountApprox > 0);

        // Time-filtered queries should throw
        Assert.Throws<InvalidOperationException>(() => store.CountEventsZeroAlloc(from: now.AddMilliseconds(-100), to: now));
        Assert.Throws<InvalidOperationException>(() => store.SumZeroAlloc(e => (double)e.Amount, from: now.AddMilliseconds(-100), to: now));
        Assert.Throws<InvalidOperationException>(() => store.AverageZeroAlloc(e => (double)e.Amount, from: now.AddMilliseconds(-100), to: now));
        Assert.Throws<InvalidOperationException>(() => store.MinZeroAlloc(e => (double)e.Amount, from: now.AddMilliseconds(-100), to: now));
        Assert.Throws<InvalidOperationException>(() => store.MaxZeroAlloc(e => (double)e.Amount, from: now.AddMilliseconds(-100), to: now));
        Assert.Throws<InvalidOperationException>(() => store.AggregateWindowZeroAlloc(e => (double)e.Amount, from: now.AddMilliseconds(-100), to: now));
    }

    [Fact]
    public void Append_WithTrackingEnabled_BehavesAsBefore()
    {
        var store = CreateStore(enableTracking: true);
        var now = DateTime.UtcNow;
        for (int i = 0; i < 10; i++)
        {
            var e = new Order(i, i + 1, now.AddMilliseconds(i * 10));
            Assert.True(store.TryAppend("k", e, e.Timestamp.Ticks));
        }

        Assert.True(store.CountApprox >= 10);

        // Queries succeed
        var from = now.AddMilliseconds(-100);
        var to = now.AddMilliseconds(200);
        var count = store.CountEventsZeroAlloc(from: from, to: to);
        var sum = store.SumZeroAlloc(e => (double)e.Amount, from: from, to: to);
        var avg = store.AverageZeroAlloc(e => (double)e.Amount, from: from, to: to);
        var min = store.MinZeroAlloc(e => (double)e.Amount, from: from, to: to);
        var max = store.MaxZeroAlloc(e => (double)e.Amount, from: from, to: to);
        var agg = store.AggregateWindowZeroAlloc(e => (double)e.Amount, from: from, to: to);

        Assert.True(count > 0);
        Assert.True(sum >= 0);
        Assert.True(avg >= 0);
        Assert.True(min is null || min >= 0);
        Assert.True(max is null || max >= 0);
        Assert.True(agg.Count >= 0);
    }
}

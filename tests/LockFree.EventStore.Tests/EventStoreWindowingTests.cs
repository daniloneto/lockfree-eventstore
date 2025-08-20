using System;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStoreWindowingTests
{
    private static EventStore<Order> CreateStore(int partitions = 2)
    {
        return new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 64,
            Partitions = partitions,
            WindowSizeTicks = TimeSpan.FromMinutes(5).Ticks
        });
    }

    [Fact]
    public void AggregateWindow_Computes_Count_Sum_Min_Max()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-6))); // outside
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-4))); // in
        store.TryAppend(new Order(3, 5m, now.AddMinutes(-3)));  // in
        store.TryAppend(new Order(4, 30m, now.AddMinutes(-2))); // in

        var res = store.AggregateWindow(now.AddMinutes(-5), now);
        Assert.Equal(3, res.Count);
        Assert.Equal(55, (int)res.Sum);
        Assert.Equal(5, (int)res.Min);
        Assert.Equal(30, (int)res.Max);
        Assert.True(res.Avg > 0);
    }

    [Fact]
    public void SumWindow_Respects_Filter_And_Range()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-6))); // out
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-4))); // in
        store.TryAppend(new Order(3, 5m, now.AddMinutes(-3)));  // in
        store.TryAppend(new Order(4, 30m, now.AddMinutes(-2))); // in

        var sum = store.SumWindow(o => o.Amount, now.AddMinutes(-5), now, e => e.Amount >= 10m);
        Assert.Equal(50m, sum);
    }

    [Fact]
    public void SnapshotViews_Filtered_By_Time_Works()
    {
        var store = CreateStore(partitions: 1);
        var now = DateTime.UtcNow;
        for (int i = 0; i < 5; i++) store.TryAppend(new Order(i, i+1, now.AddMinutes(-i)));

        var views = store.SnapshotViews(now.AddMinutes(-3), now);
        Assert.Single(views);
        Assert.True(views[0].Count <= 4);
        Assert.False(views[0].IsEmpty);
    }
}

using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStoreBuilderTests
{
    private static Order O(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    [Fact]
    public void Builder_Creates_Store_With_Disabled_WindowTracking_And_Throws_On_TimeQueries()
    {
        var builder = new EventStoreBuilder<Order>()
            .WithPartitions(2)
            .WithCapacity(32)
            .WithTimestampSelector(new OrderTimestampSelector())
            .WithEnableWindowTracking(false);
        var store = builder.Create();

        var now = DateTime.UtcNow;
        for (int i = 0; i < 5; i++)
        {
            store.TryAppend("k", O(i, i + 1, now), now.Ticks); // uses key path
        }
        Assert.True(store.CountApprox >= 5);

        // Time filtered query must throw because tracking disabled
        Assert.Throws<InvalidOperationException>(() => store.CountEventsZeroAlloc(from: now.AddSeconds(-1), to: now));
    }

    [Fact]
    public void Builder_Uses_Defaults_When_Not_Specified()
    {
        var store = new EventStoreBuilder<int>()
            .WithEnableWindowTracking(true) // leave capacity and partitions as defaults
            .Create();

        Assert.True(store.Partitions > 0);
        Assert.True(store.Capacity > 0);
    }

    [Fact]
    public void Builder_With_Explicit_Capacity_Sets_TotalCapacity()
    {
        var capacity = 100;
        var store = new EventStoreBuilder<int>()
            .WithCapacity(capacity)
            .WithPartitions(4)
            .Create();
        // Capacity should equal explicit value, not CapacityPerPartition * Partitions
        Assert.Equal(capacity, store.Capacity);
    }
}
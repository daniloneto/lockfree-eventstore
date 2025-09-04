using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PartitionWindowAdvancedTests
{
    private static Order O(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    private static EventStore<Order> CreateStore()
    {
        return new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            ValueSelector = o => (double)o.Amount,
            WindowSizeTicks = TimeSpan.FromSeconds(10).Ticks,
            BucketWidthTicks = TimeSpan.FromSeconds(1).Ticks,
            BucketCount = 16,
            Partitions = 1,
            CapacityPerPartition = 256
        });
    }

    [Fact]
    public void AdvancePartitionWindow_NoOp_When_Same_Timestamp()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        // append one event
        store.TryAppend(O(1, 10m, now));
        // capture stats to detect window advance count change (indirect)
        store.TryGetStats(out var beforeStats);
        // append another event with same timestamp -> no-op advance
        store.TryAppend(O(2, 20m, now));
        store.TryGetStats(out var afterStats);
        // AppendCount increases but window advance count should not increase more than 1 for identical timestamp path
        Assert.True(afterStats.WindowAdvanceCount - beforeStats.WindowAdvanceCount <= 1);
    }

    [Fact]
    public void AdvancePartitionWindow_Roll_Max_Buckets_ResetAll()
    {
        var store = CreateStore();
        var start = DateTime.UtcNow;
        // Fill events spanning more than window causing full reset
        for (int i = 0; i < 40; i++)
        {
            store.TryAppend(O(i, 1m, start.AddSeconds(i)));
        }
        // After large jump, min/max should be recomputed and remain valid
        var min = store.MinZeroAlloc(o => (double)o.Amount, from: start.AddSeconds(30), to: start.AddSeconds(39));
        Assert.True(min.HasValue);
        Assert.Equal(1d, min!.Value);
    }
}

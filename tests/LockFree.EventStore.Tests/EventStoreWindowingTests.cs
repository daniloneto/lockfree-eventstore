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

    private static EventStore<Order> CreateSmallBucketStore(out long windowSizeTicks, out long bucketWidthTicks, int bucketCount = 4)
    {
        windowSizeTicks = TimeSpan.FromSeconds(40).Ticks;
        bucketWidthTicks = windowSizeTicks / bucketCount;
        return new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            Partitions = 1,
            BucketCount = bucketCount,
            WindowSizeTicks = windowSizeTicks,
            ValueSelector = o => (double)o.Amount
        });
    }

    private static DateTime AlignedBase(long bucketWidthTicks)
    {
        var nowTicks = DateTime.UtcNow.Ticks;
        var aligned = nowTicks - (nowTicks % bucketWidthTicks);
        return new DateTime(aligned, DateTimeKind.Utc);
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

        var res = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: now.AddMinutes(-5), to: now);
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

        var sum = store.SumZeroAlloc(o => (double)o.Amount, from: now.AddMinutes(-5), to: now, filter: (e, _) => e.Amount >= 10m);
        Assert.Equal(50.0, sum);
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

    [Fact]
    public void WindowAdvance_LongGap_Reset_AlignsHeadAndCounts()
    {
        var store = CreateSmallBucketStore(out var windowSize, out var width, bucketCount: 4);
        var baseTime = AlignedBase(width);

        // Seed far in the past to initialize
        var seed = baseTime;
        store.TryAppend(new Order(0, 1m, seed));

        // Large gap to force advanceBuckets >= bucketCount (reset-all branch)
        var far = seed.AddTicks(width * 8); // 8 > bucketCount (4)
        store.TryAppend(new Order(1, 2m, far));

        // Append event exactly at the current window start (should map to head bucket)
        var atWindowStart = new DateTime(far.Ticks - windowSize, DateTimeKind.Utc);
        store.TryAppend(new Order(2, 4m, atWindowStart));

        var res = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: atWindowStart, to: far);
        Assert.Equal(2, res.Count); // seed is out of window
        Assert.Equal(6.0, res.Sum, 5);
    }

    [Fact]
    public void WindowAdvance_PartialBoundary_NoBucketAdvance_RealignsHead()
    {
        var store = CreateSmallBucketStore(out var windowSize, out var width, bucketCount: 4);
        var baseTime = AlignedBase(width);

        // Choose t2 so that (t2 - windowSize) is one tick before a bucket boundary
        var t2 = new DateTime(baseTime.Ticks + windowSize - 1, DateTimeKind.Utc);

        // First event initializes buckets
        store.TryAppend(new Order(10, 1m, t2));

        // Event exactly at window start
        var start = new DateTime(t2.Ticks - windowSize, DateTimeKind.Utc);
        store.TryAppend(new Order(11, 2m, start));

        var r1 = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: start, to: t2);
        Assert.Equal(2, r1.Count);
        Assert.Equal(3.0, r1.Sum, 5);

        // Advance by less than a full bucket width; the previous window-start event is now outside the window
        var t3 = t2.AddTicks(2);
        store.TryAppend(new Order(12, 3m, t3));
        var r2 = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: new DateTime(t3.Ticks - windowSize, DateTimeKind.Utc), to: t3);
        Assert.Equal(2, r2.Count); // start event dropped; t2 and t3 remain
        Assert.Equal(4.0, r2.Sum, 5); // 1 + 3

        // Now advance enough to evict exactly one bucket; the event at previous window start should remain out
        var t4 = t2.AddTicks(width + 2);
        store.TryAppend(new Order(13, 4m, t4));
        var r3 = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: new DateTime(t4.Ticks - windowSize, DateTimeKind.Utc), to: t4);
        Assert.Equal(3, r3.Count); // 1 (t2) + 3 (t3) + 4 (t4)
        Assert.Equal(8.0, r3.Sum, 5);
    }
}

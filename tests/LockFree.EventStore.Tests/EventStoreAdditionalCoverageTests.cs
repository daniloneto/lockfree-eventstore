using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStoreAdditionalCoverageTests
{
    private static Order O(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    [Fact]
    public void TryAppend_To_Invalid_Partition_Throws()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 8
        });
        Assert.Throws<ArgumentOutOfRangeException>(() => store.TryAppend(O(1, 1, DateTime.UtcNow), -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => store.TryAppend(O(1, 1, DateTime.UtcNow), 2));
    }

    [Fact]
    public void OnCapacityReached_And_OnEventDiscarded_Are_Invoked_When_Full()
    {
        var discarded = new List<Order>();
        var capacityReached = 0;
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 2,
            OnEventDiscarded = e => discarded.Add(e),
            OnCapacityReached = () => capacityReached++
        });

        // Fill to capacity
        store.TryAppend(O(1, 10, DateTime.UtcNow));
        store.TryAppend(O(2, 20, DateTime.UtcNow));
        Assert.True(store.IsFull);

        // Next append must overwrite/discard oldest and trigger hooks
        store.TryAppend(O(3, 30, DateTime.UtcNow));

        Assert.True(discarded.Count >= 1);
        Assert.True(capacityReached >= 1);
        Assert.True(store.IsFull);
    }

    [Fact]
    public void SnapshotFilteredZeroAlloc_Streams_Chunks()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 32
        });

        var now = DateTime.UtcNow;
        for (int i = 0; i < 10; i++)
            store.TryAppend(O(i, i, now));

        var received = new List<Order>();
        store.SnapshotFilteredZeroAlloc(o => o.Amount >= 5, chunk =>
        {
            for (int i = 0; i < chunk.Length; i++) received.Add(chunk[i]);
        }, chunkSize: 3);

        Assert.Equal(5, received.Count);
        Assert.All(received, o => Assert.True(o.Amount >= 5));
    }

    [Fact]
    public void SnapshotTimeFilteredZeroAlloc_Filters_By_Timestamp()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 32,
            TimestampSelector = new OrderTimestampSelector()
        });

        var now = DateTime.UtcNow;
        store.TryAppend(O(1, 1, now.AddMinutes(-10)));
        store.TryAppend(O(2, 2, now.AddMinutes(-5)));
        store.TryAppend(O(3, 3, now));

        var from = now.AddMinutes(-6);
        var to = now.AddMinutes(-1);
        var ids = new List<int>();
        store.SnapshotTimeFilteredZeroAlloc(from, to, chunk =>
        {
            for (int i = 0; i < chunk.Length; i++) ids.Add(chunk[i].Id);
        }, chunkSize: 2);

        Assert.Single(ids);
        Assert.Contains(2, ids);
    }

    [Fact]
    public void AggregateWindowZeroAlloc_Uses_Bucket_FastPath_When_Possible()
    {
        var options = new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 128,
            TimestampSelector = new OrderTimestampSelector(),
            ValueSelector = o => (double)o.Amount,
            WindowSizeTicks = TimeSpan.FromMinutes(5).Ticks,
            EnableWindowTracking = true,
            // Make bucket math deterministic: 5 buckets of 1 minute each
            BucketCount = 5,
            BucketWidthTicks = TimeSpan.FromMinutes(1).Ticks
        };
        var store = new EventStore<Order>(options);

        // Use a single fixed timestamp so no bucket evictions occur between appends
        var t0 = DateTime.UtcNow;

        // Warm-up: initialize buckets on all partitions
        store.TryAppend(O(0, 0, t0), partition: 0);
        store.TryAppend(O(0, 0, t0), partition: 1);

        // Append a few events within the same bucket deterministically (partition 0)
        store.TryAppend(O(1, 10, t0), partition: 0);
        store.TryAppend(O(2, 20, t0), partition: 0);
        store.TryAppend(O(3, 5, t0), partition: 0);

        var from = t0.AddMinutes(-5);
        var to = t0;
        var res = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: from, to: to);

        // Non-zero aggregate expected and basic bounds
        Assert.True(res.Count >= 2);
        Assert.True(res.Sum >= 30);
        Assert.True(res.Min <= 10);
        Assert.True(res.Max >= 20);
    }

    [Fact]
    public void TryAppend_With_KeyId_And_Timestamp_Updates_Buckets()
    {
        var options = new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 64,
            TimestampSelector = new OrderTimestampSelector(),
            ValueSelector = o => (double)o.Amount,
            WindowSizeTicks = TimeSpan.FromMinutes(2).Ticks,
            EnableWindowTracking = true
        };
        var store = new EventStore<Order>(options);

        var keyId = store.GetOrCreateKeyId("k");
        var t0 = DateTime.UtcNow.AddMinutes(-1);
        store.TryAppend(keyId, O(1, 7, t0), t0.Ticks);
        store.TryAppend(keyId, O(2, 8, t0.AddSeconds(10)), t0.AddSeconds(10).Ticks);

        var agg = store.AggregateWindowZeroAlloc(o => (double)o.Amount, from: t0.AddMinutes(-1), to: t0.AddMinutes(1));
        Assert.True(agg.Count >= 2);
        Assert.True(agg.Sum >= 15);
    }

    [Fact]
    public void SnapshotZeroAlloc_Uses_Padded_Path_When_Enabled()
    {
        var stats = new List<StoreStats>();
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 16,
            EnableFalseSharingProtection = true,
            OnStatsUpdated = s => stats.Add(s)
        });

        var now = DateTime.UtcNow;
        for (int i = 0; i < 5; i++) store.TryAppend(O(i, i, now));

        var seen = 0;
        store.SnapshotZeroAlloc(span => { seen += span.Length; }, chunkSize: 2);

        Assert.Equal(5, seen);
        Assert.True(stats.Any(s => s.SnapshotBytesExposed > 0));
    }

    [Fact]
    public void Purge_Without_TimestampSelector_Throws()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 8
        });
        Assert.Throws<InvalidOperationException>(() => store.Purge(DateTime.UtcNow));
    }

    [Fact]
    public void SnapshotTimeFilteredZeroAlloc_Without_TimestampSelector_Throws()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 8
        });
        Assert.Throws<InvalidOperationException>(() => store.SnapshotTimeFilteredZeroAlloc(DateTime.UtcNow.AddMinutes(-1), DateTime.UtcNow, _ => { }));
    }

    [Fact]
    public void WindowAdvance_Increments_Counter_For_Edge_Cases()
    {
        var options = new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 64,
            TimestampSelector = new OrderTimestampSelector(),
            ValueSelector = o => (double)o.Amount,
            EnableWindowTracking = true,
            WindowSizeTicks = TimeSpan.FromMinutes(4).Ticks,
            BucketCount = 4,
            BucketWidthTicks = TimeSpan.FromMinutes(1).Ticks
        };
        var store = new EventStore<Order>(options);
        var key = store.GetOrCreateKeyId("p");

        var t0 = DateTime.UtcNow.AddMinutes(-10);

        // Initialize buckets - first append does not increment advance counter
        store.TryAppend(key, O(1, 1, t0), t0.Ticks);
        store.TryGetStats(out var s0);
        var startCount = s0.WindowAdvanceCount;

        // Less than one bucket advance -> increments once
        var t1 = t0.AddSeconds(10);
        store.TryAppend(key, O(2, 2, t1), t1.Ticks);
        store.TryGetStats(out var s1);

        // Jump ahead >= bucketCount buckets -> increments again
        var t2 = t0.AddMinutes(10);
        store.TryAppend(key, O(3, 3, t2), t2.Ticks);
        store.TryGetStats(out var s2);

        // Normal advance of 1 bucket -> increments again
        var t3 = t2.AddMinutes(1);
        store.TryAppend(key, O(4, 4, t3), t3.Ticks);
        store.TryGetStats(out var s3);

        Assert.True(s1.WindowAdvanceCount >= startCount + 1);
        Assert.True(s2.WindowAdvanceCount >= s1.WindowAdvanceCount + 1);
        Assert.True(s3.WindowAdvanceCount >= s2.WindowAdvanceCount + 1);
    }

    [Fact]
    public void IsEmpty_And_IsFull_Across_Partitions()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 2
        });

        var now = DateTime.UtcNow;
        Assert.True(store.IsEmpty);
        Assert.False(store.IsFull);

        // Fill only partition 0
        store.TryAppend(O(1, 1, now), partition: 0);
        Assert.False(store.IsEmpty);
        Assert.False(store.IsFull);

        store.TryAppend(O(2, 2, now), partition: 0); // partition 0 becomes full
        Assert.True(store.IsFull);

        store.Clear();
        Assert.True(store.IsEmpty);
        Assert.False(store.IsFull);
    }
}

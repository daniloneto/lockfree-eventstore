using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Reflection;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

/// <summary>
/// Additional tests that target rarely exercised branches of EventStore&lt;TEvent&gt;
/// with the goal of pushing line coverage as high as possible.
/// </summary>
public class EventStoreFullCoverageTests
{
    private static Order NewOrder(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    [Fact]
    public void Generic_Aggregations_IntAndDecimal_Paths()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 64,
            TimestampSelector = new OrderTimestampSelector(),
            // No window tracking needed here
        });

        var now = DateTime.UtcNow;
        store.TryAppend(NewOrder(1, 10m, now));
        store.TryAppend(NewOrder(2, 20m, now.AddMilliseconds(1)));
        store.TryAppend(NewOrder(3, 5m,  now.AddMilliseconds(2)));

        // Cover generic Min/Max (TResult) overloads using decimal
        var minDec = store.MinZeroAlloc<decimal>(o => o.Amount);
        var maxDec = store.MaxZeroAlloc<decimal>(o => o.Amount);
        Assert.Equal(5m, minDec);
        Assert.Equal(20m, maxDec);

        // Cover generic Sum/Average (TValue) overloads using int
        var sumInt = store.SumZeroAlloc<int>(o => (int)o.Amount);
        var avgInt = store.AverageZeroAlloc<int>(o => (int)o.Amount);
        Assert.Equal(35, sumInt);
        Assert.Equal(35.0/3.0, avgInt, 5);
    }

    [Fact]
    public void TryAppendBatch_KeyId_SingleKey_Path()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 32,
            TimestampSelector = new OrderTimestampSelector()
        });
        var keyId = store.GetOrCreateKeyId("customer-1");
        var now = DateTime.UtcNow;
        var batch = new[]
        {
            NewOrder(1, 1m, now),
            NewOrder(2, 2m, now.AddMilliseconds(1)),
            NewOrder(3, 3m, now.AddMilliseconds(2))
        };
        var written = store.TryAppendBatch(keyId, batch);
        Assert.Equal(batch.Length, written);
    }

    [Fact]
    public void TryAppendBatch_Tuple_KeyId_Event_Path()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 4,
            CapacityPerPartition = 64,
            TimestampSelector = new OrderTimestampSelector()
        });
        var k1 = store.GetOrCreateKeyId("k1");
        var k2 = store.GetOrCreateKeyId("k2");
        var now = DateTime.UtcNow;
        var tuples = new (KeyId KeyId, Order Event)[]
        {
            (k1, NewOrder(1, 10m, now)),
            (k2, NewOrder(2, 20m, now.AddMilliseconds(1))),
            (k1, NewOrder(3, 30m, now.AddMilliseconds(2)))
        };
        var written = store.TryAppendBatch(tuples);
        Assert.Equal(tuples.Length, written);
    }

    [Fact]
    public void TryAppendBatch_Empty_ReturnsZero()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 8
        });
        ReadOnlySpan<int> empty = ReadOnlySpan<int>.Empty;
        var written = store.TryAppendBatch(empty);
        Assert.Equal(0, written);
    }

    [Fact]
    public void Sampling_OnStatsUpdated_PowerOfTwo_And_NonPowerOfTwo()
    {
        var collectedPower2 = new List<StoreStats>();
        var storePower2 = new EventStore<int>(new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 128,
            StatsUpdateInterval = 8,
            OnStatsUpdated = s => collectedPower2.Add(s)
        });
        // Append 9 items: expect callbacks at counts 8 only (sampling)
        for (int i = 1; i <= 9; i++) storePower2.TryAppend(i);
        Assert.Contains(collectedPower2, s => s.AppendCount == 8);
        Assert.DoesNotContain(collectedPower2, s => s.AppendCount == 7); // ensures bitmask path executed

        var collectedNonPower = new List<StoreStats>();
        var storeNonPower = new EventStore<int>(new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 128,
            StatsUpdateInterval = 6, // not power of two
            OnStatsUpdated = s => collectedNonPower.Add(s)
        });
        for (int i = 1; i <= 7; i++) storeNonPower.TryAppend(i);
        Assert.Contains(collectedNonPower, s => s.AppendCount == 6);
        Assert.DoesNotContain(collectedNonPower, s => s.AppendCount == 5);
    }

    [Fact]
    public void Sampling_OnStatsUpdated_Batch_Boundary_Cross()
    {
        var collected = new List<StoreStats>();
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 128,
            StatsUpdateInterval = 10,
            OnStatsUpdated = s => collected.Add(s)
        });
        // First batch of 5 (no boundary)
        store.TryAppendBatch(stackalloc int[] {1,2,3,4,5});
        // Second batch of 6 crosses boundary (from 5 -> 11)
        store.TryAppendBatch(stackalloc int[] {6,7,8,9,10,11});
        Assert.Contains(collected, s => s.AppendCount >= 10);
    }

    [Fact]
    public void ClearAll_Resets_KeyMap()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            Partitions = 2,
            CapacityPerPartition = 16,
            TimestampSelector = new OrderTimestampSelector()
        });
        var now = DateTime.UtcNow;
        store.TryAppend("k1", NewOrder(1, 1m, now));
        store.TryAppend("k2", NewOrder(2, 2m, now));
        Assert.True(store.RegisteredKeysCount >= 2);
        store.ClearAll();
        Assert.Equal(0, store.RegisteredKeysCount);
        Assert.True(store.IsEmpty);
    }

    [Fact]
    public void SnapshotZeroAlloc_Standard_Path_Covers_ProcessStandardPartitionSnapshot()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 32
        });
        for (int i = 0; i < 5; i++) store.TryAppend(i);
        var seen = 0;
        store.SnapshotZeroAlloc(span => seen += span.Length, chunkSize: 2);
        Assert.Equal(5, seen);
    }

    [Fact]
    public void EnsureBucketsInitialized_Idempotent_Path()
    {
        // Use small window/buckets and force second call to EnsureBucketsInitialized (buckets already allocated)
        var options = new EventStoreOptions<Order>
        {
            Partitions = 1,
            CapacityPerPartition = 32,
            TimestampSelector = new OrderTimestampSelector(),
            WindowSizeTicks = TimeSpan.FromSeconds(2).Ticks,
            BucketCount = 4,
            ValueSelector = o => (double)o.Amount,
            EnableWindowTracking = true
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;
        store.TryAppend(NewOrder(1, 1m, now)); // initializes buckets
        // Call private EnsureBucketsInitialized again via reflection to exercise early-return path
        var mi = typeof(EventStore<Order>).GetMethod("EnsureBucketsInitialized", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(mi);
        var stateField = typeof(EventStore<Order>).GetField("_windowStates", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(stateField);
        var arr = (PartitionWindowState[])stateField!.GetValue(store)!;
        object[] args = { 0, arr[0], now.Ticks };
        mi!.Invoke(store, args); // Should not throw; path executed
        // Replace back updated struct (since struct passed by ref via reflection copies). Not needed for coverage but keep semantics.
        arr[0] = (PartitionWindowState)args[1]!;
    }
}

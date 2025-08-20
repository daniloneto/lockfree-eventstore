using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class SpecializedEventStoreZeroAllocAndStatsTests
{
    private static Event E(int key, double v, long t) => new(new KeyId(key), v, t);

    private static double[] Values(ReadOnlySpan<Event> span)
    {
        var arr = new double[span.Length];
        for (int i = 0; i < span.Length; i++) arr[i] = span[i].Value;
        return arr;
    }

    [Fact]
    public void SnapshotZeroAlloc_Processes_All_Events_In_Chunks()
    {
        var store = new SpecializedEventStore(capacity: 40, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 20; i++) store.Add(E(i % 4, i, t0 + i));

        var chunks = new List<double[]>();
        store.SnapshotZeroAlloc(span => chunks.Add(Values(span)), chunkSize: 3);

        Assert.True(chunks.Count >= 7); // 3+3+3+3+3+3+2 (order across partitions not guaranteed)
        Assert.Equal(20, chunks.Sum(c => c.Length));
    }

    [Fact]
    public void EnumerateSnapshotZeroAlloc_Produces_Chunks()
    {
        var store = new SpecializedEventStore(capacity: 24, partitions: 3);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 12; i++) store.Add(E(i % 3, i, t0 + i));

        var total = 0;
        store.EnumerateSnapshotZeroAlloc(span => total += span.Length, chunkSize: 4);
        Assert.Equal(12, total);
    }

    [Fact]
    public void AggregateByKeyZeroAlloc_Works()
    {
        var store = new SpecializedEventStore(capacity: 64, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        // key 1 => 1+2+3 = 6; key 2 => 10+20 = 30
        store.Add(E(1, 1, t0 + 1));
        store.Add(E(1, 2, t0 + 2));
        store.Add(E(1, 3, t0 + 3));
        store.Add(E(2, 10, t0 + 4));
        store.Add(E(2, 20, t0 + 5));

        var received = new List<KeyValuePair<KeyId, double[]>>();
        store.AggregateByKeyZeroAlloc(span =>
        {
            // capture values per key as a small array for assertion; values are totals
            foreach (var kv in span)
            {
                received.Add(new KeyValuePair<KeyId, double[]>(kv.Key, new[] { kv.Value }));
            }
        }, chunkSize: 1);

        var dict = received
            .GroupBy(k => k.Key)
            .ToDictionary(g => g.Key, g => g.SelectMany(x => x.Value).Sum());

        Assert.Equal(6d, dict[new KeyId(1)]);
        Assert.Equal(30d, dict[new KeyId(2)]);
    }

    [Fact]
    public void AggregateByKeyZeroAlloc_With_Time_Range_Works()
    {
        var store = new SpecializedEventStore(capacity: 64, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        // key 1 => in-range 2+3; key 2 => in-range 10
        store.Add(E(1, 1, t0 + 1));
        store.Add(E(1, 2, t0 + 2));
        store.Add(E(1, 3, t0 + 3));
        store.Add(E(2, 10, t0 + 2));
        store.Add(E(2, 20, t0 + 5));

        var from = new DateTime(t0 + 2);
        var to = new DateTime(t0 + 3);
        var collected = new Dictionary<KeyId, double>();
        store.AggregateByKeyZeroAlloc(from, to, span =>
        {
            foreach (var kv in span)
            {
                if (collected.TryGetValue(kv.Key, out var existing)) collected[kv.Key] = existing + kv.Value;
                else collected[kv.Key] = kv.Value;
            }
        }, chunkSize: 2);

        Assert.Equal(5d, collected[new KeyId(1)]);
        Assert.Equal(10d, collected[new KeyId(2)]);
    }

    [Fact]
    public void Add_Overloads_Work_And_Capacity_Computed()
    {
        var store = new SpecializedEventStore(capacity: 10, partitions: 4); // per-partition = 2, total capacity = 8
        var now = DateTime.UtcNow;
        store.Add(new KeyId(1), 1.5, now);
        store.Add(new KeyId(1), 2.5, now.Ticks);
        Assert.Equal(2, store.CountApprox);
        Assert.Equal(8, store.Capacity);
    }

    [Fact]
    public void QueryZeroAlloc_Variants_Work()
    {
        var store = new SpecializedEventStore(capacity: 64, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 10; i++) store.Add(E(1, i, t0 + i));

        var fromOnly = new List<double>();
        store.QueryZeroAlloc(span => fromOnly.AddRange(Values(span)), from: new DateTime(t0 + 6), chunkSize: 3);
        Assert.Equal(new[] { 6d, 7d, 8d, 9d }, fromOnly.OrderBy(x => x).ToArray());

        var toOnly = new List<double>();
        store.QueryZeroAlloc(span => toOnly.AddRange(Values(span)), to: new DateTime(t0 + 3), chunkSize: 2);
        Assert.Equal(new[] { 0d, 1d, 2d, 3d }, toOnly.OrderBy(x => x).ToArray());
    }

    [Fact]
    public void QueryByKeyZeroAlloc_Variants_Work()
    {
        var store = new SpecializedEventStore(capacity: 32, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 10; i++) store.Add(E(i % 2 == 0 ? 5 : 6, i, t0 + i));

        var list = new List<double>();
        store.QueryByKeyZeroAlloc(new KeyId(5), span => list.AddRange(Values(span)), chunkSize: 4);
        Assert.Equal(new[] { 0d, 2d, 4d, 6d, 8d }, list);

        list.Clear();
        store.QueryByKeyZeroAlloc(new KeyId(5), span => list.AddRange(Values(span)), from: new DateTime(t0 + 4), chunkSize: 2);
        Assert.Equal(new[] { 4d, 6d, 8d }, list);

        list.Clear();
        store.QueryByKeyZeroAlloc(new KeyId(5), span => list.AddRange(Values(span)), to: new DateTime(t0 + 4), chunkSize: 3);
        Assert.Equal(new[] { 0d, 2d, 4d }, list);
    }
}

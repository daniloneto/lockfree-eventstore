using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class SpecializedEventStoreAdvancedTests
{
    // Campos static readonly para uso nos testes
    public static readonly double[] ExpectedValues_82 = new[] { 4d, 6d, 8d };
    public static readonly double[] ExpectedValues_88 = new[] { 5d, 6d, 7d };
    public static readonly double[] ExpectedValues_110 = new[] { 4d, 5d, 6d, 7d, 8d, 9d };
    public static readonly double[] ExpectedValues_AllKey = new[] { 0d, 2d, 4d, 6d, 8d };
    public static readonly double[] ExpectedValues_ToOnly = new[] { 0d, 2d, 4d };
    public static readonly double[] ExpectedValues_ByRange = new[] { 4d, 6d };

    private static Event E(int key, double v, long t) => new(new KeyId(key), v, t);
    private static double[] ValuesFrom(ReadOnlySpan<Event> span)
    {
        var arr = new double[span.Length];
        for (int i = 0; i < span.Length; i++) arr[i] = span[i].Value;
        return arr;
    }

    [Fact]
    public void AddRange_Span_And_IEnumerable_Chunking_Work()
    {
        var store = new SpecializedEventStore(capacity: 64, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;

        // Validate partition mapping for the keys used to avoid cross-partition eviction assumptions
        var partCount = store.Partitions;
        var p0 = Partitioners.ForKeyIdSimple(new KeyId(0), partCount);
        var p1 = Partitioners.ForKeyIdSimple(new KeyId(1), partCount);
        var p2 = Partitioners.ForKeyIdSimple(new KeyId(2), partCount);
        var p99 = Partitioners.ForKeyIdSimple(new KeyId(99), partCount);
        Assert.Equal(4, partCount);
        Assert.Equal(3, new HashSet<int> { p0, p1, p2 }.Count);
        Assert.DoesNotContain(p99, new[] { p0, p1, p2 });

        // Span overload
        var arr = Enumerable.Range(0, 10).Select(i => E(i % 3, i, t0 + i)).ToArray();
        store.AddRange(arr.AsSpan());

        // Snapshot per-partition counts before adding the 99-key batch
        var before = store.GetPartitionStatistics().ToDictionary(s => s.PartitionIndex, s => s.Count);

        // IEnumerable overload forcing chunking path (not array or list)
        static IEnumerable<Event> Gen(long startTicks, int count)
        {
            for (int i = 0; i < count; i++) yield return new Event(new KeyId(99), i, startTicks + i);
        }
        // Use 16 to avoid overflowing a single partition (capacity per partition = 16)
        store.AddRange(Gen(t0 + 100, 16));

        // Verify per-partition counts: only the 99-key partition increased by 16
        var after = store.GetPartitionStatistics().ToDictionary(s => s.PartitionIndex, s => s.Count);
        before.TryGetValue(p0, out var b0); after.TryGetValue(p0, out var a0); Assert.Equal(b0, a0);
        before.TryGetValue(p1, out var b1); after.TryGetValue(p1, out var a1); Assert.Equal(b1, a1);
        before.TryGetValue(p2, out var b2); after.TryGetValue(p2, out var a2); Assert.Equal(b2, a2);
        before.TryGetValue(p99, out var b99); after.TryGetValue(p99, out var a99); Assert.Equal(b99 + 16, a99);

        Assert.Equal(26, store.CountApprox);
        Assert.False(store.IsEmpty);
    }

    [Fact]
    public void Query_ByKey_And_Time_Range_Variants_Work()
    {
        var store = new SpecializedEventStore(capacity: 32, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        var key = new KeyId(7);

        // Insert across keys and times
        for (int i = 0; i < 10; i++) store.Add(E(i % 2 == 0 ? 7 : 8, i, t0 + i));

        // All by key
        var allKeyVals = new List<double>();
        store.QueryByKeyZeroAlloc(key, span =>
        {
            for (int i = 0; i < span.Length; i++) allKeyVals.Add(span[i].Value);
        });
        Assert.Equal(ExpectedValues_AllKey, allKeyVals.ToArray());

        // From only
        var fromOnlyVals = new List<double>();
        store.QueryByKeyZeroAlloc(key, span =>
        {
            for (int i = 0; i < span.Length; i++) fromOnlyVals.Add(span[i].Value);
        }, from: new DateTime(t0 + 4));
        Assert.Equal(ExpectedValues_82, fromOnlyVals.ToArray());

        // To only
        var toOnlyVals = new List<double>();
        store.QueryByKeyZeroAlloc(key, span =>
        {
            for (int i = 0; i < span.Length; i++) toOnlyVals.Add(span[i].Value);
        }, to: new DateTime(t0 + 4));
        Assert.Equal(ExpectedValues_ToOnly, toOnlyVals.ToArray());

        // By range
        var byRangeVals = new List<double>();
        store.QueryByKeyZeroAlloc(key, span =>
        {
            for (int i = 0; i < span.Length; i++) byRangeVals.Add(span[i].Value);
        }, from: new DateTime(t0 + 3), to: new DateTime(t0 + 7));
        Assert.Equal(ExpectedValues_ByRange, byRangeVals.ToArray());

        // All by range across partitions
        var allByRangeVals = new List<double>();
        store.QueryZeroAlloc(span =>
        {
            for (int i = 0; i < span.Length; i++) allByRangeVals.Add(span[i].Value);
        }, from: new DateTime(t0 + 5), to: new DateTime(t0 + 7));
        var ordered = allByRangeVals.OrderBy(x => x).ToArray();
        Assert.Equal(ExpectedValues_88, ordered);
    }

    [Fact]
    public void QueryZeroAlloc_And_ByKeyZeroAlloc_Produce_Correct_Chunks()
    {
        var store = new SpecializedEventStore(capacity: 64, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 12; i++) store.Add(E(1, i, t0 + i));

        var chunks = new List<double[]>();
        store.QueryZeroAlloc(span => chunks.Add(ValuesFrom(span)), chunkSize: 5);
        Assert.True(chunks.Count >= 3); // e.g., 5,5,2 chunks
        Assert.Equal(12, chunks.Sum(c => c.Length));
        // Lock in chunk-size contract: each chunk non-empty and <= chunkSize
        Assert.All(chunks, c => Assert.InRange(c.Length, 1, 5));
        var maxLen = chunks.Max(c => c.Length);
        Assert.InRange(maxLen, 1, 5);

        chunks.Clear();
        store.QueryByKeyZeroAlloc(new KeyId(1), span => chunks.Add(ValuesFrom(span)),
            from: new DateTime(t0 + 4), to: new DateTime(t0 + 9), chunkSize: 3);
        Assert.Equal(ExpectedValues_110, chunks.SelectMany(x => x).ToArray());

        // Lock in chunk-size contract for ByKey path as well
        Assert.All(chunks, c => Assert.InRange(c.Length, 1, 3));
        var maxLen2 = chunks.Max(c => c.Length);
        Assert.InRange(maxLen2, 1, 3);
    }

    [Fact]
    public void AggregateByKey_With_Time_Filter_Works()
    {
        var store = new SpecializedEventStore(capacity: 64, partitions: 4);
        var t0 = new DateTime(2024, 1, 1).Ticks;

        // Key 1
        store.Add(E(1, 1, t0 + 1));
        store.Add(E(1, 2, t0 + 2));
        store.Add(E(1, 3, t0 + 5));
        // Key 2
        store.Add(E(2, 10, t0 + 3));
        store.Add(E(2, 20, t0 + 6));

        var agg = store.AggregateByKey(new DateTime(t0 + 2), new DateTime(t0 + 5));
        Assert.Equal(5d, agg[new KeyId(1)]); // 2 + 3
        Assert.Equal(10d, agg[new KeyId(2)]); // only at t0+3
    }

    [Fact]
    public void GetPartitionStatistics_And_Purge_Work()
    {
        var store = new SpecializedEventStore(capacity: 20, partitions: 2);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 10; i++) store.Add(E(i % 2, i, t0 + i));

        var stats = store.GetPartitionStatistics().ToArray();
        Assert.Equal(2, stats.Length);
        Assert.Equal(store.CountApprox, stats.Sum(s => s.Count));

        var purged = store.Purge(new DateTime(t0 + 5));
        Assert.True(purged >= 5);
        Assert.All(store.EnumerateSnapshot(), e => Assert.True(e.TimestampTicks >= t0 + 5));
    }

    [Fact]
    public void Discard_Updates_Statistics_When_Handler_Provided()
    {
        // Provide a non-null discard handler to enable internal discard accounting
        var store = new SpecializedEventStore(capacity: 6, partitions: 2, onEventDiscarded: _ => { });
        var t0 = new DateTime(2024, 1, 1).Ticks;

        // Overfill to cause discards
        for (int i = 0; i < 20; i++) store.Add(E(1, i, t0 + i));

        Assert.True(store.Statistics.TotalAppended >= 20);
        Assert.True(store.Statistics.TotalDiscarded > 0);
    }
}

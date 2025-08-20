using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class SpecializedEventStoreAdvancedTests
{
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

        // Span overload
        var arr = Enumerable.Range(0, 10).Select(i => E(i % 3, i, t0 + i)).ToArray();
        store.AddRange(arr.AsSpan());

        // IEnumerable overload forcing chunking path (not array or list)
        static IEnumerable<Event> Gen(long startTicks, int count)
        {
            for (int i = 0; i < count; i++) yield return new Event(new KeyId(99), i, startTicks + i);
        }
        // Use 16 to avoid overflowing a single partition (capacity per partition = 16)
        store.AddRange(Gen(t0 + 100, 16));

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

        var allKey = store.Query(key).Select(e => e.Value).ToArray();
        Assert.Equal(new[] { 0d, 2d, 4d, 6d, 8d }, allKey);

        var fromOnly = store.Query(key, from: new DateTime(t0 + 4)).Select(e => e.Value).ToArray();
        Assert.Equal(new[] { 4d, 6d, 8d }, fromOnly);

        var toOnly = store.Query(key, to: new DateTime(t0 + 4)).Select(e => e.Value).ToArray();
        Assert.Equal(new[] { 0d, 2d, 4d }, toOnly);

        var byRange = store.Query(key, new DateTime(t0 + 3), new DateTime(t0 + 7)).Select(e => e.Value).ToArray();
        Assert.Equal(new[] { 4d, 6d }, byRange);

        var allByRange = store.Query(new DateTime(t0 + 5), new DateTime(t0 + 7))
            .Select(e => e.Value)
            .OrderBy(x => x)
            .ToArray();
        Assert.Equal(new[] { 5d, 6d, 7d }, allByRange);
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

        chunks.Clear();
        store.QueryByKeyZeroAlloc(new KeyId(1), span => chunks.Add(ValuesFrom(span)),
            from: new DateTime(t0 + 4), to: new DateTime(t0 + 9), chunkSize: 3);
        Assert.Equal(new[] { 4d, 5d, 6d, 7d, 8d, 9d }, chunks.SelectMany(x => x).ToArray());
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
        Assert.All(store.Query(), e => Assert.True(e.TimestampTicks >= t0 + 5));
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

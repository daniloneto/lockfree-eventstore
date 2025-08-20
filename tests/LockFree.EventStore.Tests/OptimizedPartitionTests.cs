using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class OptimizedPartitionTests
{
    private static Event E(int key, double v, long t) => new(new KeyId(key), v, t);

    private static List<Event> Enumerate(PartitionView<Event> view)
    {
        var list = new List<Event>(view.Count);
        foreach (var e in view) list.Add(e);
        return list;
    }

    [Fact]
    public void AoS_TryEnqueue_And_GetView_WrapAround_Order()
    {
        var discarded = 0;
        var p = new OptimizedPartition(capacity: 5, StorageLayout.AoS, _ => discarded++);
        var t0 = new DateTime(2024, 1, 1).Ticks;

        for (int i = 0; i < 3; i++) Assert.True(p.TryEnqueue(E(1, i + 1, t0 + i)));
        var view1 = p.GetView();
        Assert.Equal(3, view1.Count);
        Assert.Equal(new[] { 1d, 2d, 3d }, Enumerate(view1).Select(e => e.Value).ToArray());

        // Force wrap and overwrite
        p.TryEnqueue(E(1, 4, t0 + 3));
        p.TryEnqueue(E(1, 5, t0 + 4));
        p.TryEnqueue(E(1, 6, t0 + 5));

        var view2 = p.GetView();
        Assert.Equal(5, view2.Count);
        Assert.Equal(new[] { 2d, 3d, 4d, 5d, 6d }, Enumerate(view2).Select(e => e.Value).ToArray());
        Assert.True(discarded >= 1);
    }

    [Fact]
    public void AoS_TryEnqueueBatch_Maintains_Order()
    {
        var p = new OptimizedPartition(capacity: 5, StorageLayout.AoS);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        var first = Enumerable.Range(0, 3).Select(i => E(1, i + 1, t0 + i)).ToArray();
        Assert.Equal(3, p.TryEnqueueBatch(first));
        var second = Enumerable.Range(3, 4).Select(i => E(1, i + 1, t0 + i)).ToArray();
        Assert.Equal(4, p.TryEnqueueBatch(second));

        var view = p.GetView();
        Assert.Equal(new[] { 3d, 4d, 5d, 6d, 7d }, Enumerate(view).Select(e => e.Value).ToArray());
    }

    [Fact]
    public void SoA_Direct_Spans_And_ZeroAlloc_Getters_Work_With_WrapAround()
    {
        var p = new OptimizedPartition(capacity: 6, StorageLayout.SoA);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 8; i++) p.TryEnqueue(E(i % 2, i + 1, t0 + i));

        // GetView constructs contiguous events
        var view = p.GetView();
        Assert.Equal(6, view.Count);
        Assert.Equal(new[] { 3d, 4d, 5d, 6d, 7d, 8d }, Enumerate(view).Select(e => e.Value).ToArray());

        // Direct spans (may copy on wrap); verify content only
        var keys = p.GetKeysSpan().ToArray();
        var vals = p.GetValuesSpan().ToArray();
        var ts = p.GetTimestampsSpan().ToArray();
        var viewArr = Enumerate(view).ToArray();
        Assert.Equal(view.Count, keys.Length);
        Assert.Equal(view.Count, vals.Length);
        Assert.Equal(view.Count, ts.Length);
        Assert.Equal(viewArr.Select(e => e.Key).ToArray(), keys);
        Assert.Equal(viewArr.Select(e => e.Value).ToArray(), vals);
        Assert.Equal(viewArr.Select(e => e.TimestampTicks).ToArray(), ts);

        // Zero-alloc getters
        List<KeyId> zKeys = new();
        p.GetKeysZeroAlloc(span => zKeys.AddRange(span.ToArray()));
        Assert.Equal(keys, zKeys.ToArray());

        List<double> zVals = new();
        p.GetValuesZeroAlloc(span => zVals.AddRange(span.ToArray()));
        Assert.Equal(vals, zVals.ToArray());

        List<long> zTs = new();
        p.GetTimestampsZeroAlloc(span => zTs.AddRange(span.ToArray()));
        Assert.Equal(ts, zTs.ToArray());
    }

    [Fact]
    public void GetViewZeroAlloc_Provides_View_For_Both_Layouts()
    {
        var t0 = new DateTime(2024, 1, 1).Ticks;
        var pAos = new OptimizedPartition(4, StorageLayout.AoS);
        var pSoa = new OptimizedPartition(4, StorageLayout.SoA);
        for (int i = 0; i < 5; i++) { pAos.TryEnqueue(E(1, i + 1, t0 + i)); pSoa.TryEnqueue(E(1, i + 1, t0 + i)); }

        pAos.GetViewZeroAlloc(view =>
        {
            Assert.Equal(4, view.Count);
            Assert.Equal(new[] { 2d, 3d, 4d, 5d }, Enumerate(view).Select(e => e.Value).ToArray());
        });

        pSoa.GetViewZeroAlloc(view =>
        {
            Assert.Equal(4, view.Count);
            Assert.Equal(new[] { 2d, 3d, 4d, 5d }, Enumerate(view).Select(e => e.Value).ToArray());
        });
    }

    [Fact]
    public void Clear_Empties_Partition()
    {
        var p = new OptimizedPartition(4, StorageLayout.AoS);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 3; i++) p.TryEnqueue(E(1, i + 1, t0 + i));
        Assert.False(p.IsEmpty);
        p.Clear();
        Assert.True(p.IsEmpty);
    }
}

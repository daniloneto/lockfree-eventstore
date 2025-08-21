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

    // Expected arrays (CA1861)
    private static readonly double[] ExpectedFirst123 = new[] { 1d, 2d, 3d };
    private static readonly double[] ExpectedWrap2To6 = new[] { 2d, 3d, 4d, 5d, 6d };
    private static readonly double[] ExpectedBatch3To7 = new[] { 3d, 4d, 5d, 6d, 7d };
    private static readonly double[] ExpectedView2To5 = new[] { 2d, 3d, 4d, 5d };
    private static readonly double[] ExpectedSoAValues = new[] { 3d, 4d, 5d, 6d, 7d, 8d };

    [Fact]
    public void AoS_TryEnqueue_And_GetView_WrapAround_Order()
    {
        var discarded = 0;
        var p = new OptimizedPartition(capacity: 5, StorageLayout.AoS, _ => discarded++);
        var t0 = new DateTime(2024, 1, 1).Ticks;

        for (int i = 0; i < 3; i++) Assert.True(p.TryEnqueue(E(1, i + 1, t0 + i)));
        var view1 = p.GetView();
        Assert.Equal(3, view1.Count);
        Assert.Equal(ExpectedFirst123, Enumerate(view1).Select(e => e.Value).ToArray());

        // Force wrap and overwrite
        p.TryEnqueue(E(1, 4, t0 + 3));
        p.TryEnqueue(E(1, 5, t0 + 4));
        p.TryEnqueue(E(1, 6, t0 + 5));

        var view2 = p.GetView();
        Assert.Equal(5, view2.Count);
        Assert.Equal(ExpectedWrap2To6, Enumerate(view2).Select(e => e.Value).ToArray());
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
        Assert.Equal(ExpectedBatch3To7, Enumerate(view).Select(e => e.Value).ToArray());
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
        Assert.Equal(ExpectedSoAValues, Enumerate(view).Select(e => e.Value).ToArray());

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
            Assert.Equal(ExpectedView2To5, Enumerate(view).Select(e => e.Value).ToArray());
        });

        pSoa.GetViewZeroAlloc(view =>
        {
            Assert.Equal(4, view.Count);
            Assert.Equal(ExpectedView2To5, Enumerate(view).Select(e => e.Value).ToArray());
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

    [Fact]
    public void SoA_Only_APIs_Throw_On_AoS_Layout()
    {
        var p = new OptimizedPartition(4, StorageLayout.AoS);
        Assert.Throws<InvalidOperationException>(() => p.GetKeysSpan());
        Assert.Throws<InvalidOperationException>(() => p.GetValuesSpan());
        Assert.Throws<InvalidOperationException>(() => p.GetTimestampsSpan());
        Assert.Throws<InvalidOperationException>(() => p.GetKeysZeroAlloc(_ => { }));
        Assert.Throws<InvalidOperationException>(() => p.GetValuesZeroAlloc(_ => { }));
        Assert.Throws<InvalidOperationException>(() => p.GetTimestampsZeroAlloc(_ => { }));
    }

    [Fact]
    public void ZeroAlloc_Getters_Do_Not_Invoke_On_Empty_SoA_And_Views_Are_Empty()
    {
        var pSoa = new OptimizedPartition(8, StorageLayout.SoA);
        int k = 0, v = 0, t = 0;
        pSoa.GetKeysZeroAlloc(_ => k++);
        pSoa.GetValuesZeroAlloc(_ => v++);
        pSoa.GetTimestampsZeroAlloc(_ => t++);
        Assert.Equal(0, k);
        Assert.Equal(0, v);
        Assert.Equal(0, t);

        // Empty views (AoS and SoA)
        var pAos = new OptimizedPartition(4, StorageLayout.AoS);
        var viewAos = pAos.GetView();
        Assert.True(viewAos.IsEmpty);
        Assert.Equal(0, viewAos.FromTicks);
        Assert.Equal(0, viewAos.ToTicks);

        pAos.GetViewZeroAlloc(view =>
        {
            Assert.True(view.IsEmpty);
            Assert.Equal(0, view.FromTicks);
            Assert.Equal(0, view.ToTicks);
        });

        var viewSoa = pSoa.GetView();
        Assert.True(viewSoa.IsEmpty);
        Assert.Equal(0, viewSoa.FromTicks);
        Assert.Equal(0, viewSoa.ToTicks);

        pSoa.GetViewZeroAlloc(view =>
        {
            Assert.True(view.IsEmpty);
            Assert.Equal(0, view.FromTicks);
            Assert.Equal(0, view.ToTicks);
        });
    }

    [Fact]
    public void GetView_Sets_FromTicks_ToTicks_For_AoS_And_SoA()
    {
        var t0 = new DateTime(2024, 1, 1).Ticks;
        var pAos = new OptimizedPartition(8, StorageLayout.AoS);
        var pSoa = new OptimizedPartition(8, StorageLayout.SoA);
        for (int i = 0; i < 3; i++) { pAos.TryEnqueue(E(1, i + 1, t0 + i)); pSoa.TryEnqueue(E(1, i + 1, t0 + i)); }

        var vAos = pAos.GetView();
        Assert.Equal(t0, vAos.FromTicks);
        Assert.Equal(t0 + 2, vAos.ToTicks);

        var vSoa = pSoa.GetView();
        Assert.Equal(t0, vSoa.FromTicks);
        Assert.Equal(t0 + 2, vSoa.ToTicks);
    }

    [Fact]
    public void GetViewZeroAlloc_Sets_Ticks_And_Works_When_Full()
    {
        var t0 = new DateTime(2024, 1, 1).Ticks;
        var pAos = new OptimizedPartition(4, StorageLayout.AoS);
        var pSoa = new OptimizedPartition(4, StorageLayout.SoA);
        for (int i = 0; i < 6; i++) { pAos.TryEnqueue(E(1, i + 1, t0 + i)); pSoa.TryEnqueue(E(1, i + 1, t0 + i)); }

        pAos.GetViewZeroAlloc(view =>
        {
            Assert.Equal(4, view.Count);
            Assert.Equal(t0 + 2, view.FromTicks);
            Assert.Equal(t0 + 5, view.ToTicks);
        });

        pSoa.GetViewZeroAlloc(view =>
        {
            Assert.Equal(4, view.Count);
            Assert.Equal(t0 + 2, view.FromTicks);
            Assert.Equal(t0 + 5, view.ToTicks);
        });
    }

    [Fact]
    public void IsFull_And_IsEmpty_Are_Accurate()
    {
        var p = new OptimizedPartition(3, StorageLayout.AoS);
        Assert.True(p.IsEmpty);
        Assert.False(p.IsFull);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 3; i++) p.TryEnqueue(E(1, i + 1, t0 + i));
        Assert.False(p.IsEmpty);
        Assert.True(p.IsFull);
        p.Clear();
        Assert.True(p.IsEmpty);
        Assert.False(p.IsFull);
    }

    [Fact]
    public void SoA_Discard_Callback_Is_Invoked_With_Overwritten_Event()
    {
        var discarded = new List<Event>();
        var p = new OptimizedPartition(3, StorageLayout.SoA, discarded.Add);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 4; i++) p.TryEnqueue(E(1, i + 1, t0 + i));
        Assert.NotEmpty(discarded);
        // As in LockFreeRingBuffer, callback observes the value written into the overwritten slot
        Assert.Contains(discarded, e => e.Value == 4d && e.TimestampTicks == t0 + 3);
    }

    [Fact]
    public void SoA_TryEnqueueBatch_NoWrap_Then_Wrap_Maintains_Order()
    {
        var p = new OptimizedPartition(6, StorageLayout.SoA);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        // No wrap
        var first = Enumerable.Range(0, 3).Select(i => E(1, i + 1, t0 + i)).ToArray();
        Assert.Equal(3, p.TryEnqueueBatch(first));
        var v1 = p.GetView();
        Assert.Equal(new[] { 1d, 2d, 3d }, Enumerate(v1).Select(e => e.Value).ToArray());
        // Wrap after second batch
        var second = Enumerable.Range(3, 5).Select(i => E(1, i + 1, t0 + i)).ToArray();
        Assert.Equal(5, p.TryEnqueueBatch(second));
        var v2 = p.GetView();
        Assert.Equal(new[] { 3d, 4d, 5d, 6d, 7d, 8d }, Enumerate(v2).Select(e => e.Value).ToArray());
    }

    [Fact]
    public void SoA_Direct_Spans_NoWrap_And_ZeroAlloc_NoWrap()
    {
        var p = new OptimizedPartition(6, StorageLayout.SoA);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 3; i++) p.TryEnqueue(E(i, i + 1, t0 + i));

        // No wrap spans
        var keys = p.GetKeysSpan();
        var vals = p.GetValuesSpan();
        var ts = p.GetTimestampsSpan();
        var keysArr = keys.ToArray();
        var valsArr = vals.ToArray();
        var tsArr = ts.ToArray();
        Assert.Equal(3, keys.Length);
        Assert.Equal(3, vals.Length);
        Assert.Equal(3, ts.Length);
        Assert.Equal(new[] { new KeyId(0), new KeyId(1), new KeyId(2) }, keysArr);
        Assert.Equal(new[] { 1d, 2d, 3d }, valsArr);
        Assert.Equal(new[] { t0, t0 + 1, t0 + 2 }, tsArr);

        // Zero-alloc getters with no wrap should call processor exactly once
        int kc = 0, vc = 0, tc = 0;
        p.GetKeysZeroAlloc(span => { kc++; Assert.Equal(keysArr, span.ToArray()); });
        p.GetValuesZeroAlloc(span => { vc++; Assert.Equal(valsArr, span.ToArray()); });
        p.GetTimestampsZeroAlloc(span => { tc++; Assert.Equal(tsArr, span.ToArray()); });
        Assert.Equal(1, kc);
        Assert.Equal(1, vc);
        Assert.Equal(1, tc);
    }

    [Fact]
    public void AoS_GetView_And_GetViewZeroAlloc_Handle_IsFull_HeadZero_SingleSegment()
    {
        var p = new OptimizedPartition(4, StorageLayout.AoS);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 4; i++) p.TryEnqueue(E(1, i + 1, t0 + i)); // exactly capacity; headIndex=0

        var view = p.GetView();
        Assert.False(view.HasWrapAround);
        Assert.Equal(new[] { 1d, 2d, 3d, 4d }, Enumerate(view).Select(e => e.Value).ToArray());
        Assert.Equal(t0, view.FromTicks);
        Assert.Equal(t0 + 3, view.ToTicks);

        p.GetViewZeroAlloc(v =>
        {
            Assert.False(v.HasWrapAround);
            Assert.Equal(new[] { 1d, 2d, 3d, 4d }, Enumerate(v).Select(e => e.Value).ToArray());
            Assert.Equal(t0, v.FromTicks);
            Assert.Equal(t0 + 3, v.ToTicks);
        });
    }
}

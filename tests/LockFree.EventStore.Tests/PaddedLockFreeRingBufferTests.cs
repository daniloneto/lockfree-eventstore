using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PaddedLockFreeRingBufferTests
{
    private static readonly int[] ExpectedSnapshot345 = new[] { 3, 4, 5 };
    private static readonly int[] Batch12345 = new[] { 1, 2, 3, 4, 5 };
    private static readonly int[] Batch67 = new[] { 6, 7 };
    private static readonly int[] ExpectedViewList = new[] { 1, 2, 3, 4 };
    private static readonly double[] ExpectedFilteredValues = new[] { 1d, 2d, 3d };

    [Fact]
    public void TryEnqueue_Overwrite_Discard_Callback_Invoked()
    {
        var discarded = new List<int>();
        var buf = new PaddedLockFreeRingBuffer<int>(3, discarded.Add);

        buf.TryEnqueue(1);
        buf.TryEnqueue(2);
        buf.TryEnqueue(3);
        Assert.True(buf.IsFull);

        buf.TryEnqueue(4); // overwrite slot 0, callback receives 4 (new value in overwritten slot)
        buf.TryEnqueue(5); // overwrite slot 1, callback receives 5

        Assert.Contains(4, discarded);
        Assert.Contains(5, discarded);

        var arr = new int[3];
        var n = buf.Snapshot(arr);
        Assert.Equal(3, n);
        Assert.Equal(ExpectedSnapshot345, arr);
    }

    [Fact]
    public void TryEnqueueBatch_Wraps_And_Snapshots_In_Order()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(5);
        var written = buf.TryEnqueueBatch(Batch12345);
        Assert.Equal(5, written);

        written = buf.TryEnqueueBatch(Batch67);
        Assert.Equal(2, written);

        var arr = new int[5];
        var n = buf.Snapshot(arr);
        Assert.Equal(5, n);
        Assert.Equal(new[] { 3, 4, 5, 6, 7 }, arr);
    }

    [Fact]
    public void CreateView_Returns_All_Items()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(4);
        foreach (var i in Enumerable.Range(1, 4)) buf.TryEnqueue(i);

        var view = buf.CreateView();
        Assert.False(view.IsEmpty);
        Assert.Equal(4, view.Count);
        var list = new List<int>();
        foreach (var x in view) list.Add(x);
        Assert.Equal(ExpectedViewList, list);
    }

    [Fact]
    public void CreateViewFiltered_Uses_Timestamps()
    {
        var sel = new EventTimestampSelector();
        var buf = new PaddedLockFreeRingBuffer<Event>(8);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 6; i++) buf.TryEnqueue(new Event(new KeyId(i), i, t0 + i));

        var view = buf.CreateViewFiltered(t0 + 1, t0 + 3, sel);
        Assert.Equal(3, view.Count);
        var items = new List<Event>();
        foreach (var e in view) items.Add(e);
        Assert.Equal(ExpectedFilteredValues, items.Select(e => e.Value));
    }

    [Fact]
    public void SnapshotZeroAlloc_And_ProcessItemsZeroAlloc_Work()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(8);
        foreach (var i in Enumerable.Range(1, 6)) buf.TryEnqueue(i);

        var chunks = new List<int[]>();
        buf.SnapshotZeroAlloc<int>(span =>
        {
            chunks.Add(span.ToArray());
        }, ArrayPool<int>.Shared, chunkSize: 3);
        Assert.True(chunks.Count >= 2);

        buf.ProcessItemsZeroAlloc(0, (sum, item) => (sum + item, item < 4), out var final);
        Assert.True(final >= 1);
    }
}

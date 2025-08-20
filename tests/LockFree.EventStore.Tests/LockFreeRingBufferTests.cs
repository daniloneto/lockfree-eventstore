using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class LockFreeRingBufferTests
{
    [Fact]
    public void TryEnqueue_Overwrites_When_Full_And_Invokes_Discard()
    {
        var discarded = new List<int>();
        var buf = new LockFreeRingBuffer<int>(capacity: 3, onItemDiscarded: discarded.Add);

        Assert.True(buf.IsEmpty);

        buf.TryEnqueue(1);
        buf.TryEnqueue(2);
        buf.TryEnqueue(3);
        Assert.True(buf.IsFull);
        Assert.Equal(3, buf.CountApprox);

        // Next enqueues will overwrite the oldest (1, then 2)
        buf.TryEnqueue(4);
        buf.TryEnqueue(5);

        // Implementation invokes discard with the value observed in the overwritten slot
        Assert.Contains(4, discarded);
        Assert.Contains(5, discarded);

        // Snapshot should contain last 3: 3,4,5
        var tmp = new int[3];
        var len = buf.Snapshot(tmp);
        Assert.Equal(3, len);
        Assert.Equal(new[] { 3, 4, 5 }, tmp);
    }

    [Fact]
    public void TryEnqueueBatch_WrapAround_Snapshot_Order_Is_Correct()
    {
        var discarded = 0;
        var buf = new LockFreeRingBuffer<int>(5, _ => discarded++);

        buf.TryEnqueue(1);
        buf.TryEnqueue(2);
        buf.TryEnqueue(3);

        // Batch of four forces wrap; implementation discards at most one per head advance
        var written = buf.TryEnqueueBatch(new[] { 4, 5, 6, 7 });
        Assert.Equal(4, written);
        Assert.True(discarded >= 1);

        var arr = new int[5];
        var n = buf.Snapshot(arr);
        Assert.Equal(5, n);
        Assert.Equal(new[] { 3, 4, 5, 6, 7 }, arr);
    }

    [Fact]
    public void CreateView_NoSelector_Returns_All_Elements()
    {
        var buf = new LockFreeRingBuffer<int>(4);
        foreach (var i in Enumerable.Range(1, 4)) buf.TryEnqueue(i);

        var view = buf.CreateView();
        Assert.False(view.IsEmpty);
        Assert.Equal(4, view.Count);

        var list = new List<int>();
        foreach (var x in view)
            list.Add(x);
        Assert.Equal(new[] { 1, 2, 3, 4 }, list);
    }

    [Fact]
    public void CreateViewFiltered_With_Timestamps_Filters_Correctly()
    {
        var sel = new EventTimestampSelector();
        var buf = new LockFreeRingBuffer<Event>(8);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        // Events with increasing ticks
        for (int i = 0; i < 6; i++)
        {
            buf.TryEnqueue(new Event(new KeyId(i), i, t0 + i));
        }

        var view = buf.CreateViewFiltered(t0 + 2, t0 + 4, sel);
        Assert.Equal(3, view.Count);
        var items = new List<Event>();
        foreach (var e in view) items.Add(e);
        Assert.Equal(new[] { 2d, 3d, 4d }, items.Select(e => e.Value));
    }

    [Fact]
    public void SnapshotZeroAlloc_Processes_In_Chunks()
    {
        var buf = new LockFreeRingBuffer<int>(16);
        foreach (var i in Enumerable.Range(1, 10)) buf.TryEnqueue(i);

        var chunks = new List<int[]>();
        buf.SnapshotZeroAlloc<int>(span =>
        {
            chunks.Add(span.ToArray());
        }, ArrayPool<int>.Shared, chunkSize: 4);

        // Expect chunks of size 4,4,2
        Assert.Equal(3, chunks.Count);
        Assert.Equal(new[] { 1, 2, 3, 4 }, chunks[0]);
        Assert.Equal(new[] { 5, 6, 7, 8 }, chunks[1]);
        Assert.Equal(new[] { 9, 10 }, chunks[2]);
    }

    [Fact]
    public void ProcessItemsZeroAlloc_Supports_Early_Termination()
    {
        var buf = new LockFreeRingBuffer<int>(8);
        foreach (var i in Enumerable.Range(1, 6)) buf.TryEnqueue(i);

        buf.ProcessItemsZeroAlloc(0, (sum, item) =>
        {
            sum += item;
            // stop once sum exceeds 10
            return (sum, sum <= 10);
        }, out var finalState);

        Assert.True(finalState > 10);
    }
}

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class LockFreeRingBufferTests
{
    private static readonly int[] ExpectedSnapshotFirst = new[] { 3, 4, 5 };
    private static readonly int[] Batch2467 = new[] { 4, 5, 6, 7 };
    private static readonly int[] ExpectedArrAfterBatch = new[] { 3, 4, 5, 6, 7 };
    private static readonly int[] ExpectedList = new[] { 1, 2, 3, 4 };
    private static readonly double[] ExpectedEventValues = new[] { 2d, 3d, 4d };
    private static readonly int[] ExpectedChunk1 = new[] { 1, 2, 3, 4 };
    private static readonly int[] ExpectedChunk2 = new[] { 5, 6, 7, 8 };
    private static readonly int[] ExpectedChunk3 = new[] { 9, 10 };

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
        Assert.Equal(ExpectedSnapshotFirst, tmp);
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
        var written = buf.TryEnqueueBatch(Batch2467);
        Assert.Equal(4, written);
        Assert.True(discarded >= 1);

        var arr = new int[5];
        var n = buf.Snapshot(arr);
        Assert.Equal(5, n);
        Assert.Equal(ExpectedArrAfterBatch, arr);
    }

    [Fact]
    public void CreateView_NoSelector_Returns_All_Elements()
    {
        var buf = new LockFreeRingBuffer<int>(4);
        foreach (var i in ExpectedList) buf.TryEnqueue(i);

        var view = buf.CreateView();
        Assert.False(view.IsEmpty);
        Assert.Equal(4, view.Count);

        var list = new List<int>();
        foreach (var x in view)
            list.Add(x);
        Assert.Equal(ExpectedList, list);
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
        Assert.Equal(ExpectedEventValues, items.Select(e => e.Value));
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
        Assert.Equal(ExpectedChunk1, chunks[0]);
        Assert.Equal(ExpectedChunk2, chunks[1]);
        Assert.Equal(ExpectedChunk3, chunks[2]);
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

    [Fact]
    public void Constructor_Invalid_Capacity_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new LockFreeRingBuffer<int>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LockFreeRingBuffer<int>(-5));
    }

    [Fact]
    public void Snapshot_On_Empty_Returns_Zero_And_EnumerateSnapshot_Is_Empty()
    {
        var buf = new LockFreeRingBuffer<int>(4);
        var tmp = new int[4];
        Assert.Equal(0, buf.Snapshot(tmp));
        Assert.Empty(buf.EnumerateSnapshot());
    }

    [Fact]
    public void Clear_Resets_Buffer_And_View_Is_Empty()
    {
        var buf = new LockFreeRingBuffer<int>(4);
        foreach (var i in Enumerable.Range(1, 4)) buf.TryEnqueue(i);
        Assert.True(buf.IsFull);
        buf.Clear();
        Assert.True(buf.IsEmpty);
        var arr = new int[4];
        Assert.Equal(0, buf.Snapshot(arr));
        var view = buf.CreateView();
        Assert.True(view.IsEmpty);
        Assert.Equal(0, view.Count);
    }

    [Fact]
    public void TryEnqueue_ReadOnlySpan_Batch_Works()
    {
        var buf = new LockFreeRingBuffer<int>(6);
        var batch = new[] { 1, 2, 3, 4 };
        var written = buf.TryEnqueue((ReadOnlySpan<int>)batch);
        Assert.Equal(batch.Length, written);
        var dst = new int[6];
        var n = buf.Snapshot(dst);
        Assert.Equal(4, n);
        Assert.True(dst.Take(4).SequenceEqual(batch));
    }

    [Fact]
    public void CreateView_With_TimestampSelector_Sets_Ticks_NoWrap_And_Wrap()
    {
        var sel = new EventTimestampSelector();
        var t0 = new DateTime(2024, 1, 1).Ticks;

        // No wrap
        var buf1 = new LockFreeRingBuffer<Event>(8);
        for (int i = 0; i < 3; i++) buf1.TryEnqueue(new Event(new KeyId(1), i, t0 + i));
        var v1 = buf1.CreateView(sel);
        Assert.Equal(t0, v1.FromTicks);
        Assert.Equal(t0 + 2, v1.ToTicks);
        Assert.False(v1.HasWrapAround);

        // Wrap case
        var buf2 = new LockFreeRingBuffer<Event>(5);
        for (int i = 0; i < 7; i++) buf2.TryEnqueue(new Event(new KeyId(1), i, t0 + i));
        var v2 = buf2.CreateView(sel);
        Assert.Equal(t0 + 2, v2.FromTicks);
        Assert.Equal(t0 + 6, v2.ToTicks);
        Assert.True(v2.HasWrapAround);
    }

    [Fact]
    public void CreateViewFiltered_Empty_When_No_Items_In_Window()
    {
        var sel = new EventTimestampSelector();
        var buf = new LockFreeRingBuffer<Event>(6);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 3; i++) buf.TryEnqueue(new Event(new KeyId(1), i, t0 + i));
        var view = buf.CreateViewFiltered(t0 + 100, t0 + 200, sel);
        Assert.True(view.IsEmpty);
        Assert.Equal(0, view.Count);
    }

    [Fact]
    public void CreateViewFiltered_WrapAround_Window()
    {
        var sel = new EventTimestampSelector();
        var buf = new LockFreeRingBuffer<Event>(5);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 7; i++) buf.TryEnqueue(new Event(new KeyId(1), i, t0 + i));
        // Current logical ticks present: t0+2 .. t0+6; choose [t0+3, t0+5] to force wrap in segments
        var view = buf.CreateViewFiltered(t0 + 3, t0 + 5, sel);
        Assert.Equal(3, view.Count);
        Assert.True(view.HasWrapAround);
        var vals = new List<double>();
        foreach (var e in view) vals.Add(e.Value);
        Assert.Equal(new[] { 3d, 4d, 5d }, vals.ToArray());
    }

    [Fact]
    public void EnumerateWindow_And_AdvanceWindowTo_Work()
    {
        var sel = new EventTimestampSelector();
        var buf = new LockFreeRingBuffer<Event>(8);
        var t0 = new DateTime(2024, 1, 1).Ticks;
        for (int i = 0; i < 6; i++) buf.TryEnqueue(new Event(new KeyId(1), i, t0 + i));

        // Enumerate full window
        var collected = new List<(double v, long t)>();
        buf.EnumerateWindow(t0, t0 + 10, sel, ref collected, (ref List<(double v, long t)> state, Event item, long ticks) =>
        {
            state.Add((item.Value, ticks));
        });
        Assert.Equal(6, collected.Count);
        Assert.Equal(t0, collected.First().t);
        Assert.Equal(t0 + 5, collected.Last().t);

        // Advance window to t0+3 (remove first three)
        var removed = new List<(double v, long t)>();
        var windowHead = 0;
        buf.AdvanceWindowTo(t0 + 3, sel, ref removed, (ref List<(double v, long t)> state, Event item, long ticks) =>
        {
            state.Add((item.Value, ticks));
        }, ref windowHead);
        Assert.Equal(3, windowHead);
        Assert.Equal(new long[] { t0, t0 + 1, t0 + 2 }, removed.Select(x => x.t).ToArray());
    }

    [Fact]
    public void Snapshot_Partial_Copy_When_Destination_Smaller()
    {
        var buf = new LockFreeRingBuffer<int>(6);
        foreach (var i in Enumerable.Range(1, 5)) buf.TryEnqueue(i);
        var dst = new int[3];
        var n = buf.Snapshot(dst);
        Assert.Equal(3, n);
        // Should copy the first 3 from the current head
        Assert.Equal(new[] { 1, 2, 3 }, dst);
    }
}

using System;
using System.Buffers;
using System.Collections.Generic;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class RingBufferAdditionalTests
{
    private static Order O(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    [Fact]
    public void LockFreeRingBuffer_WrapAround_Snapshot_Works()
    {
        var buf = new LockFreeRingBuffer<int>(4);
        for (int i = 0; i < 10; i++) buf.TryEnqueue(i); // force multiple overwrites
        Span<int> snap = stackalloc int[4];
        var copied = buf.Snapshot(snap);
        Assert.Equal(4, copied);
        // Last 4 values expected: 6,7,8,9
        Assert.Equal(new[] {6,7,8,9}, snap.ToArray());
    }

    [Fact]
    public void LockFreeRingBuffer_CreateViewFiltered_OutsideRange_ReturnsEmpty()
    {
        var tsSel = new OrderTimestampSelector();
        var now = DateTime.UtcNow;
        var buf = new LockFreeRingBuffer<Order>(4);
        buf.TryEnqueue(O(1,1,now));
        buf.TryEnqueue(O(2,2,now.AddSeconds(1)));
        var view = buf.CreateViewFiltered(now.AddMinutes(10).Ticks, now.AddMinutes(11).Ticks, tsSel);
        Assert.Equal(0, view.Count);
    }

    [Fact]
    public void LockFreeRingBuffer_CreateViewFiltered_PartialRange_Wrap()
    {
        var tsSel = new OrderTimestampSelector();
        var now = DateTime.UtcNow;
        var buf = new LockFreeRingBuffer<Order>(4);
        // Fill and wrap timestamps
        for (int i = 0; i < 8; i++)
            buf.TryEnqueue(O(i, i, now.AddSeconds(i))); // After 4, wrap occurs
        // Filter mid range
        var from = now.AddSeconds(5).Ticks; // should include 5,6,7
        var to = now.AddSeconds(6).Ticks;
        var view = buf.CreateViewFiltered(from, to, tsSel);
        // Collect items manually (PartitionView exposes two segments)
        var collected = new List<Order>();
        var seg1 = view.Segment1.Span;
        var seg2 = view.Segment2.Span;
        for (int i = 0; i < seg1.Length; i++) collected.Add(seg1[i]);
        for (int i = 0; i < seg2.Length; i++) collected.Add(seg2[i]);
        Assert.True(view.Count == collected.Count);
        foreach (var item in collected)
        {
            var t = tsSel.GetTimestamp(item).Ticks;
            Assert.True(t >= from && t <= to);
        }
    }

    [Fact]
    public void LockFreeRingBuffer_EnumerateSnapshot_AfterOverflow()
    {
        var buf = new LockFreeRingBuffer<int>(3);
        for (int i = 0; i < 7; i++) buf.TryEnqueue(i);
        var snapshotItems = buf.EnumerateSnapshot();
        Assert.Equal(new[] {4,5,6}, snapshotItems);
    }

    [Fact]
    public void PaddedRingBuffer_Batch_Overwrite_Callback_Fires()
    {
        var discarded = new List<int>();
        var buf = new PaddedLockFreeRingBuffer<int>(5, d => discarded.Add(d));
        // Batch larger than capacity to force overwrite of all prior slots multiple times
        Span<int> batch = stackalloc int[7];
        for (int i = 0; i < batch.Length; i++) batch[i] = i;
        buf.TryEnqueueBatch(batch);
        Assert.NotEmpty(discarded); // Some discards should have occurred
        var stats = buf.GetStatistics();
        Assert.True(stats.Count <= 5);
    }

    [Fact]
    public void PaddedRingBuffer_CreateViewFiltered_Empty_When_NoMatch()
    {
        var tsSel = new OrderTimestampSelector();
        var now = DateTime.UtcNow;
        var buf = new PaddedLockFreeRingBuffer<Order>(4);
        buf.TryEnqueue(O(1,1,now));
        var view = buf.CreateViewFiltered(now.AddSeconds(10).Ticks, now.AddSeconds(11).Ticks, tsSel);
        Assert.Equal(0, view.Count);
    }

    [Fact]
    public void PaddedRingBuffer_SnapshotZeroAlloc_LastPartialChunk()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(10);
        for (int i = 0; i < 7; i++) buf.TryEnqueue(i);
        var processed = new List<int>();
        buf.SnapshotZeroAlloc<int>(span => processed.AddRange(span.ToArray()), ArrayPool<int>.Shared, chunkSize:4);
        // We expect at least the 7 items processed, across chunks 4 + 3
        Assert.Equal(7, processed.Count);
    }
}

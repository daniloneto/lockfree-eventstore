using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PaddedLockFreeRingBufferCoverageTests
{
    [Fact]
    public void TryEnqueueBatch_Overflow_Invokes_Discard_Callback_Advances_Head_And_Increments_Epoch()
    {
        var discarded = new List<int>();
        var buf = new PaddedLockFreeRingBuffer<int>(5, discarded.Add);

        // Fill exactly to capacity
        buf.TryEnqueueBatch(new[] { 1, 2, 3, 4, 5 });
        var before = buf.GetStatistics();
        Assert.Equal(5, before.Count);
        Assert.True(buf.IsFull);

        // Enqueue a batch that overflows by 4
        var written = buf.TryEnqueueBatch(new[] { 6, 7, 8, 9 });
        Assert.Equal(4, written);

        // Discard callback should be invoked once per overwritten slot with the NEW values
        Assert.Equal(new[] { 6, 7, 8, 9 }, discarded);

        // Head should advance by 4, count should remain at capacity, epoch must not decrease
        var after = buf.GetStatistics();
        Assert.Equal(before.Head + 4, after.Head);
        Assert.Equal(5, after.Count);
        Assert.True(after.Epoch >= before.Epoch);

        // Snapshot reflects the newest 5 items
        var arr = new int[5];
        var n = buf.Snapshot(arr);
        Assert.Equal(5, n);
        Assert.Equal(new[] { 5, 6, 7, 8, 9 }, arr);
    }

    [Fact]
    public void CreateView_With_TimestampSelector_Populates_From_To_Ticks()
    {
        var t0 = new DateTime(2024, 1, 1);
        var sel = new EventTimestampSelector();
        var buf = new PaddedLockFreeRingBuffer<Event>(4);

        // No wrap: headIndex == 0, tailIndex == 0 with count == capacity
        for (int i = 0; i < 4; i++)
        {
            buf.TryEnqueue(new Event(new KeyId(i), i + 1, t0.AddTicks(i).Ticks));
        }

        var view = buf.CreateView(sel);
        Assert.False(view.IsEmpty);
        Assert.Equal(4, view.Count);
        Assert.Equal(t0.Ticks, view.FromTicks);
        Assert.Equal(t0.AddTicks(3).Ticks, view.ToTicks);

        var values = new List<double>();
        foreach (var e in view) values.Add(e.Value);
        Assert.Equal(new[] { 1d, 2d, 3d, 4d }, values);
    }

    [Fact]
    public void CreateViewFiltered_With_Wrap_Returns_Two_Segments_And_Correct_Items()
    {
        var sel = new EventTimestampSelector();
        var buf = new PaddedLockFreeRingBuffer<Event>(5);

        // Enqueue 7 events so the buffer is full and headIndex != 0
        for (int i = 1; i <= 7; i++)
        {
            buf.TryEnqueue(new Event(new KeyId(i), i, i)); // use ticks == i for simplicity
        }

        // Select timestamps 5..7 (inclusive). This spans across the physical end of the array.
        var view = buf.CreateViewFiltered(5, 7, sel);
        Assert.Equal(3, view.Count);
        Assert.True(view.HasWrapAround);

        var got = new List<double>();
        foreach (var e in view) got.Add(e.Value);
        Assert.Equal(new[] { 5d, 6d, 7d }, got);
    }

    [Fact]
    public void Snapshot_With_Smaller_Destination_Copies_Partial()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(6);
        buf.TryEnqueueBatch(new[] { 1, 2, 3, 4, 5 });

        var dst = new int[3];
        var n = buf.Snapshot(dst);
        Assert.Equal(3, n);
        Assert.Equal(new[] { 1, 2, 3 }, dst);
    }

    [Fact]
    public void ProcessItemsZeroAlloc_Processes_All_When_Not_Terminating()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(8);
        buf.TryEnqueueBatch(new[] { 1, 2, 3, 4, 5 });

        buf.ProcessItemsZeroAlloc(0, (sum, item) => (sum + item, true), out var final);
        Assert.Equal(1 + 2 + 3 + 4 + 5, final);
    }
}

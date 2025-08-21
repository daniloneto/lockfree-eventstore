using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class LockFreeEventRingBufferTests
{
    private static readonly double[] ExpectedSnapshot = new[]{2d,3d,4d};
    private static readonly double[] ExpectedValues = new[]{2d,3d,4d,5d,6d};
    private static readonly double[] ExpectedByTime = new[]{2d,3d,4d};
    private static readonly double[] ExpectedByKey = new[]{3d};
    private static readonly double[] ExpectedByKeyAndTime = new[]{3d};

    [Fact]
    public void TryEnqueue_Overwrite_Invokes_Discard()
    {
        var discarded = new List<Event>();
        var buf = new LockFreeEventRingBuffer(3, discarded.Add);
        var t0 = new DateTime(2024,1,1).Ticks;

        buf.TryEnqueue(new Event(new KeyId(1), 1, t0));
        buf.TryEnqueue(new Event(new KeyId(2), 2, t0+1));
        buf.TryEnqueue(new Event(new KeyId(3), 3, t0+2));
        Assert.True(buf.IsFull);

        buf.TryEnqueue(new Event(new KeyId(4), 4, t0+3)); // discard first
        Assert.Contains(discarded, e => e.Value == 1);

        var snap = buf.EnumerateSnapshot().ToArray();
        Assert.Equal(ExpectedSnapshot, snap.Select(e=>e.Value));
    }

    [Fact]
    public void TryEnqueueBatch_Writes_All_And_Maintains_Order()
    {
        var buf = new LockFreeEventRingBuffer(5);
        var t0 = new DateTime(2024,1,1).Ticks;
        var batch = Enumerable.Range(0,4).Select(i => new Event(new KeyId(i), i, t0+i)).ToArray();
        var written = buf.TryEnqueueBatch(batch);
        Assert.Equal(4, written);

        var next = Enumerable.Range(4,3).Select(i => new Event(new KeyId(i), i, t0+i)).ToArray();
        buf.TryEnqueueBatch(next);

        var values = buf.EnumerateSnapshot().Select(e=>e.Value).ToArray();
        Assert.Equal(ExpectedValues, values);
    }

    [Fact]
    public void EnumerateSnapshot_Filtered_By_Time_And_Key()
    {
        var buf = new LockFreeEventRingBuffer(8);
        var key = new KeyId(7);
        var t0 = new DateTime(2024,1,1).Ticks;
        for (int i=0;i<6;i++) buf.TryEnqueue(new Event(i==3?key:new KeyId(i), i, t0+i));

        var byTime = buf.EnumerateSnapshot(t0+2, t0+4).Select(e=>e.Value).ToArray();
        Assert.Equal(ExpectedByTime, byTime);

        var byKey = buf.EnumerateSnapshot(key).Select(e=>e.Value).ToArray();
        Assert.Equal(ExpectedByKey, byKey);

        var byKeyAndTime = buf.EnumerateSnapshot(key, t0, t0+10).Select(e=>e.Value).ToArray();
        Assert.Equal(ExpectedByKeyAndTime, byKeyAndTime);
    }

    [Fact]
    public void Purge_Removes_Older_Events()
    {
        var buf = new LockFreeEventRingBuffer(8);
        var t0 = new DateTime(2024,1,1).Ticks;
        for (int i=0;i<6;i++) buf.TryEnqueue(new Event(new KeyId(i), i, t0+i));

        var purged = buf.Purge(t0+3);
        Assert.True(purged >= 3);

        var left = buf.EnumerateSnapshot().Select(e=>e.TimestampTicks).ToArray();
        Assert.True(left.All(t => t >= t0+3));
    }

    [Fact]
    public void SnapshotZeroAlloc_Yields_Chunks()
    {
        var buf = new LockFreeEventRingBuffer(10);
        var t0 = new DateTime(2024,1,1).Ticks;
        for (int i=0;i<7;i++) buf.TryEnqueue(new Event(new KeyId(i), i, t0+i));

        var got = new List<Event[]>();
        buf.SnapshotZeroAlloc(span => got.Add(span.ToArray()), chunkSize: 3);

        Assert.Equal(3, got.Count);
        Assert.Equal(3, got[0].Length);
        Assert.Equal(3, got[1].Length);
        Assert.Single(got[2]);
    }
}

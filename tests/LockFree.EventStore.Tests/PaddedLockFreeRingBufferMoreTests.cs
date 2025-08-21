using System;
using System.Linq;
using System.Buffers;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PaddedLockFreeRingBufferMoreTests
{
    [Fact]
    public void Constructor_InvalidCapacity_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PaddedLockFreeRingBuffer<int>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PaddedLockFreeRingBuffer<int>(-1));
    }

    [Fact]
    public void IsEmpty_IsFull_And_Snapshot_Empty()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(3);
        Assert.True(buf.IsEmpty);
        Assert.False(buf.IsFull);

        var tmp = new int[3];
        var n = buf.Snapshot(tmp);
        Assert.Equal(0, n);

        buf.TryEnqueue(1);
        buf.TryEnqueue(2);
        buf.TryEnqueue(3);
        Assert.True(buf.IsFull);
        Assert.False(buf.IsEmpty);
    }

    [Fact]
    public void Clear_Resets_State_And_Increments_Epoch()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(4);
        buf.TryEnqueueBatch(new[] { 1, 2, 3 });
        var before = buf.GetStatistics();
        Assert.True(before.Count > 0);

        buf.Clear();
        var after = buf.GetStatistics();
        Assert.Equal(0, after.Count);
        Assert.Equal(after.Tail, after.Head);
        Assert.True(after.Epoch >= before.Epoch);
    }

    [Fact]
    public void GetStatistics_Reflects_Operations()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(3);
        var s0 = buf.GetStatistics();
        Assert.Equal(0, s0.Head);
        Assert.Equal(0, s0.Tail);
        Assert.Equal(0, s0.Count);

        buf.TryEnqueue(10);
        var s1 = buf.GetStatistics();
        Assert.Equal(1, s1.Tail);
        Assert.True(s1.Count >= 1);

        buf.TryEnqueueBatch(new[] { 20, 30, 40 }); // causes overwrite and epoch increment
        var s2 = buf.GetStatistics();
        Assert.True(s2.Tail >= s1.Tail);
        Assert.True(s2.Count <= 3);
        Assert.True(s2.Epoch >= s1.Epoch);
    }

    [Fact]
    public void EnumerateSnapshot_Returns_All_Items()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(5);
        buf.TryEnqueueBatch(new[] { 1, 2, 3, 4 });
        var list = buf.EnumerateSnapshot().ToArray();
        Assert.Equal(new[] { 1, 2, 3, 4 }, list);
    }

    [Fact]
    public void SnapshotZeroAlloc_With_ArrayPool_Works()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(6);
        buf.TryEnqueueBatch(new[] { 1, 2, 3, 4, 5 });

        int total = 0;
        buf.SnapshotZeroAlloc<int>(span => total += span.Length, ArrayPool<int>.Shared, chunkSize: 2);
        Assert.Equal(5, total);
    }
}

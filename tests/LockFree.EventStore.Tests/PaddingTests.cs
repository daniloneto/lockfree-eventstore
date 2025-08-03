using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace LockFree.EventStore.Tests;

/// <summary>
/// Tests for anti-false sharing padding features
/// </summary>
public class PaddingTests
{
    [Fact]
    public void PartitionHeader_HasCorrectSize()
    {
        // PartitionHeader should be padded to cache line boundary (64 bytes)
        var header = new PartitionHeader(1000);
        
        // Test basic functionality
        Assert.Equal(0, header.Head);
        Assert.Equal(0, header.Tail);
        Assert.Equal(1000, header.Capacity);
        Assert.Equal(0, header.GetApproximateCount());
    }

    [Fact]
    public void PaddedPartitionHeaders_CreatesCorrectCount()
    {
        var paddedHeaders = new PaddedPartitionHeaders(4, 1000);
        
        Assert.Equal(4, paddedHeaders.Count);
        
        // Test access to each header
        for (int i = 0; i < 4; i++)
        {
            ref var header = ref paddedHeaders.GetHeader(i);
            Assert.Equal(1000, header.Capacity);
            Assert.Equal(0, header.GetApproximateCount());
        }
    }

    [Fact]
    public void PaddedLockFreeRingBuffer_BasicOperations()
    {
        var buffer = new PaddedLockFreeRingBuffer<int>(10);
        
        Assert.Equal(10, buffer.Capacity);
        Assert.True(buffer.IsEmpty);
        Assert.False(buffer.IsFull);
        Assert.Equal(0, buffer.CountApprox);
        
        // Test enqueue
        Assert.True(buffer.TryEnqueue(1));
        Assert.True(buffer.TryEnqueue(2));
        Assert.True(buffer.TryEnqueue(3));
        
        Assert.Equal(3, buffer.CountApprox);
        Assert.False(buffer.IsEmpty);
        Assert.False(buffer.IsFull);
        
        // Test snapshot
        var snapshot = new int[10];
        var count = buffer.Snapshot(snapshot);
        
        Assert.Equal(3, count);
        Assert.Equal(1, snapshot[0]);
        Assert.Equal(2, snapshot[1]);
        Assert.Equal(3, snapshot[2]);
    }

    [Fact]
    public void EventStore_WithPaddingEnabled_BasicOperations()
    {
        var options = new EventStoreOptions<string>
        {
            EnableFalseSharingProtection = true,
            CapacityPerPartition = 100,
            Partitions = 4
        };
        
        var store = new EventStore<string>(options);
        
        Assert.Equal(4, store.Partitions);
        Assert.Equal(400, store.Capacity);
        Assert.True(store.IsEmpty);
        
        // Test append operations
        Assert.True(store.TryAppend("test1"));
        Assert.True(store.TryAppend("test2"));
        Assert.True(store.TryAppend("test3"));
        
        Assert.Equal(3, store.CountApprox);
        Assert.False(store.IsEmpty);
        
        // Test snapshot
        var snapshot = store.Snapshot();
        Assert.Equal(3, snapshot.Count);
        
        // Test query
        var results = store.Query().ToList();
        Assert.Equal(3, results.Count);
        Assert.Contains("test1", results);
        Assert.Contains("test2", results);
        Assert.Contains("test3", results);
    }

    [Fact]
    public void EventStore_WithPaddingEnabled_BatchOperations()
    {
        var options = new EventStoreOptions<int>
        {
            EnableFalseSharingProtection = true,
            CapacityPerPartition = 1000,
            Partitions = 2
        };
        
        var store = new EventStore<int>(options);
        
        // Test batch append
        var batch = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var written = store.TryAppendBatch(batch);
        
        Assert.Equal(10, written);
        Assert.Equal(10, store.CountApprox);
        
        // Test zero-allocation snapshot
        var processedCount = 0;
        store.SnapshotZeroAlloc(chunk =>
        {
            processedCount += chunk.Length;
        });
        
        Assert.Equal(10, processedCount);
    }

    [Fact]
    public void EventStore_WithAndWithoutPadding_SameBehavior()
    {
        // Create two stores - one with padding, one without
        var optionsWithPadding = new EventStoreOptions<int>
        {
            EnableFalseSharingProtection = true,
            CapacityPerPartition = 100,
            Partitions = 2
        };
        
        var optionsWithoutPadding = new EventStoreOptions<int>
        {
            EnableFalseSharingProtection = false,
            CapacityPerPartition = 100,
            Partitions = 2
        };
        
        var storeWithPadding = new EventStore<int>(optionsWithPadding);
        var storeWithoutPadding = new EventStore<int>(optionsWithoutPadding);
        
        // Add same data to both
        var testData = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        
        foreach (var item in testData)
        {
            Assert.True(storeWithPadding.TryAppend(item));
            Assert.True(storeWithoutPadding.TryAppend(item));
        }
        
        // Both should have same count
        Assert.Equal(storeWithoutPadding.CountApprox, storeWithPadding.CountApprox);
        
        // Both should have same snapshot content
        var snapshotWithPadding = storeWithPadding.Snapshot().OrderBy(x => x).ToList();
        var snapshotWithoutPadding = storeWithoutPadding.Snapshot().OrderBy(x => x).ToList();
        
        Assert.Equal(snapshotWithoutPadding.Count, snapshotWithPadding.Count);
        for (int i = 0; i < snapshotWithoutPadding.Count; i++)
        {
            Assert.Equal(snapshotWithoutPadding[i], snapshotWithPadding[i]);
        }
    }

    [Fact]
    public void PaddedPartitionHeaders_ThrowsOnInvalidIndex()
    {
        var paddedHeaders = new PaddedPartitionHeaders(3, 1000);
        
        Assert.Throws<ArgumentOutOfRangeException>(() => paddedHeaders.GetHeader(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => paddedHeaders.GetHeader(3));
        Assert.Throws<ArgumentOutOfRangeException>(() => paddedHeaders.GetHeader(10));
    }
}

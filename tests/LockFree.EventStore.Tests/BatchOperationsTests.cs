using System;
using System.Linq;
using Xunit;

namespace LockFree.EventStore.Tests;

public class BatchOperationsTests
{
    // Static expected arrays for asserts (CA1861)
    private static readonly int[] Expected_1_to_5 = new[] { 1, 2, 3, 4, 5 };
    private static readonly int[] Expected_4_to_8 = new[] { 4, 5, 6, 7, 8 };

    [Fact]
    public void TryAppendBatch_Works_Correctly()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 2
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Create a batch of orders
        var batch = new Order[]
        {
            new Order(1, 10m, now.AddMinutes(-5)),
            new Order(2, 20m, now.AddMinutes(-4)),
            new Order(3, 30m, now.AddMinutes(-3)),
            new Order(4, 40m, now.AddMinutes(-2)),
            new Order(5, 50m, now.AddMinutes(-1))
        };

        var written = store.TryAppendBatch(batch);

        Assert.Equal(5, written);
        Assert.Equal(5, store.Count);
    }

    [Fact]
    public void TryAppendBatch_Single_Partition_Works()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100,
            Partitions = 3
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Create a batch of orders for specific partition
        var batch = new Order[]
        {
            new Order(1, 10m, now.AddMinutes(-3)),
            new Order(2, 20m, now.AddMinutes(-2)),
            new Order(3, 30m, now.AddMinutes(-1))
        };

        var written = store.TryAppendBatch(batch, 0);

        Assert.Equal(3, written);
        Assert.Equal(3, store.Count);
    }

    [Fact]
    public void TryEnqueueBatch_RingBuffer_Optimized()
    {
        var buffer = new LockFreeRingBuffer<int>(10);

        var batch = new int[] { 1, 2, 3, 4, 5 };
        var written = buffer.TryEnqueueBatch(batch);

        Assert.Equal(5, written);
        Assert.Equal(5, buffer.CountApprox);

        // Verify all items were written correctly
        var snapshot = new int[10];
        var count = buffer.Snapshot(snapshot);
        
        Assert.Equal(5, count);
        Assert.Equal(Expected_1_to_5, snapshot.Take(5).ToArray());
    }

    [Fact]
    public void TryEnqueueBatch_Handles_Wraparound()
    {
        var buffer = new LockFreeRingBuffer<int>(5);

        // Fill buffer to capacity
        var firstBatch = new int[] { 1, 2, 3, 4, 5 };
        buffer.TryEnqueueBatch(firstBatch);

        // Add more items to cause wraparound
        var secondBatch = new int[] { 6, 7, 8 };
        var written = buffer.TryEnqueueBatch(secondBatch);

        Assert.Equal(3, written);
        Assert.Equal(5, buffer.CountApprox); // Still at capacity

        // Verify newest items are in buffer
        var snapshot = new int[5];
        var count = buffer.Snapshot(snapshot);
        
        Assert.Equal(5, count);
        Assert.Contains(6, snapshot);
        Assert.Contains(7, snapshot);
        Assert.Contains(8, snapshot);
    }

    [Fact]
    public void Optimized_Snapshot_With_Spans_Works()
    {
        var buffer = new LockFreeRingBuffer<int>(10);

        // Add some test data
        for (int i = 1; i <= 7; i++)
        {
            buffer.TryEnqueue(i);
        }

        // Test optimized snapshot
        Span<int> destination = stackalloc int[10];
        var count = buffer.Snapshot(destination);

        Assert.Equal(7, count);
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(i + 1, destination[i]);
        }
    }

    [Fact]
    public void Optimized_Snapshot_Handles_Wraparound()
    {
        var buffer = new LockFreeRingBuffer<int>(5);

        // Fill buffer beyond capacity to cause wraparound
        for (int i = 1; i <= 8; i++)
        {
            buffer.TryEnqueue(i);
        }

        // Test optimized snapshot with wraparound
        Span<int> destination = stackalloc int[5];
        var count = buffer.Snapshot(destination);

        Assert.Equal(5, count);
        // Should contain the last 5 items: 4, 5, 6, 7, 8
        Assert.Equal(Expected_4_to_8, destination.ToArray());
    }    [Fact]
    public void TryAppendAll_Works_With_Ring_Buffer_Behavior()
    {
        var options = new EventStoreOptions<int>
        {
            CapacityPerPartition = 3, 
            Partitions = 1 
        };
        var store = new EventStore<int>(options);

        // Create a batch larger than capacity
        var batch = new int[] { 1, 2, 3, 4, 5 };
        var written = store.TryAppendAll(batch);

        // Ring buffer accepts all items (overwrites oldest)
        Assert.Equal(5, written);
        Assert.Equal(3, store.Count); // But only capacity items remain

        // Verify that we have the last 3 items
        var snapshot = store.Snapshot();
        Assert.Equal(3, snapshot.Count);
        Assert.Contains(3, snapshot);
        Assert.Contains(4, snapshot);
        Assert.Contains(5, snapshot);
    }

    [Fact]
    public void BatchOperations_Performance_Better_Than_Individual()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 10000,
            Partitions = 4
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Create a large batch of orders
        var batch = new Order[1000];
        for (int i = 0; i < batch.Length; i++)
        {
            batch[i] = new Order(i, i * 1.5m, now.AddSeconds(-i));
        }

        // Test batch operation
        var batchWritten = store.TryAppendBatch(batch);
        Assert.Equal(1000, batchWritten);

        // Clear for individual test
        store.Clear();

        // Test individual operations
        int individualWritten = 0;
        foreach (var order in batch)
        {
            if (store.TryAppend(order))
                individualWritten++;
        }

        Assert.Equal(1000, individualWritten);
        
        // Both should achieve the same result
        Assert.Equal(batchWritten, individualWritten);
    }
}

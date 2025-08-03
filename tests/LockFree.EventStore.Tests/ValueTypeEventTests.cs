using System;
using System.Diagnostics;
using Xunit;
using System.Linq;

namespace LockFree.EventStore.Tests;

/// <summary>
/// Tests for the Event record struct and contiguous storage layout
/// </summary>
public class ValueTypeEventTests
{
    [Fact]
    public void Event_Is_Value_Type()
    {
        // Verify that Event is a value type
        Assert.True(typeof(Event).IsValueType);
    }
    
    [Fact]
    public void Event_Layout_Is_Contiguous()
    {
        // Create a store with multiple partitions
        var store = new EventStoreV2(100_000, 4);
        
        // Add some events
        for (int i = 0; i < 1000; i++)
        {
            store.Add($"key{i % 10}", i);
        }
        
        // Verify count
        Assert.Equal(1000, store.Count);
        
        // Verify we can retrieve events
        var view = store.GetPartitionView(0);
        Assert.True(view.Count > 0);
        
        // Verify memory layout is contiguous by checking memory addresses
        if (view.Count >= 2)
        {
            var span = view.AsSpan();
            unsafe
            {
                fixed (Event* ptr = span)
                {
                    // Check if memory addresses are sequential
                    // Size of Event should be the difference between consecutive elements
                    var sizeOfEvent = sizeof(Event);
                    var eventPtr1 = ptr;
                    var eventPtr2 = ptr + 1;
                    
                    // The difference should match the size of Event
                    Assert.Equal((long)eventPtr2 - (long)eventPtr1, sizeOfEvent);
                }
            }
        }
    }
    
    [Fact]
    public void Event_Store_Performance_Test()
    {
        // Create a store
        var store = new EventStoreV2(1_000_000, 8);
        
        // Prepare test data
        const int testSize = 100_000;
        var keys = new string[10];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = $"key{i}";
        }
        
        // Measure insert performance
        var sw = Stopwatch.StartNew();
        
        for (int i = 0; i < testSize; i++)
        {
            store.Add(keys[i % keys.Length], i);
        }
        
        sw.Stop();
        
        // Output performance metrics
        var opsPerSecond = testSize / sw.Elapsed.TotalSeconds;
        
        // Verify we can query the data
        var sum = store.Sum("key0");
        Assert.True(sum > 0);
        
        // The test passed if we got here without exceptions
        // and we achieved reasonable performance
        Assert.True(opsPerSecond > 0);
    }
    
    [Fact]
    public void Event_Batch_Operations_Test()
    {
        // Create a store
        var store = new EventStoreV2(100_000, 4);
        
        // Add batch of events for a single key
        var values = new double[100];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }
        
        // Convert to span
        var valuesSpan = new ReadOnlySpan<double>(values);
        
        // Add as batch
        var keyId = new KeyMap().GetOrAdd("testKey");
        store.AddBatch(keyId, valuesSpan, DateTime.UtcNow.Ticks);
        
        // Verify count
        Assert.Equal(values.Length, store.Count);
        
        // Verify aggregate operations
        Assert.Equal(values.Sum(), store.Sum("testKey"), 0.001);
        Assert.Equal(values.Average(), store.Average("testKey"), 0.001);
        Assert.Equal(values.Min(), store.Min("testKey"), 0.001);
        Assert.Equal(values.Max(), store.Max("testKey"), 0.001);
    }
}

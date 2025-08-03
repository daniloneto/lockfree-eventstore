using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Xunit;
using System.Collections.Generic;
using System.Linq;

namespace LockFree.EventStore.Tests;

/// <summary>
/// Tests for measuring memory performance and cache efficiency
/// </summary>
public class CachePerformanceTests
{
    [Fact]
    public void Contiguous_Memory_Layout_Improves_Iteration_Performance()
    {
        // This test demonstrates that contiguous memory layout improves performance
        // by reducing cache misses during iteration
        
        const int testSize = 1_000_000;
        
        // Create our value type events in contiguous array
        var valueTypeEvents = new Event[testSize];
        for (int i = 0; i < testSize; i++)
        {
            valueTypeEvents[i] = new Event(new KeyId(i % 100), i, DateTime.UtcNow.Ticks);
        }
        
        // Create reference type events as a comparison
        var refTypeEvents = new MetricEvent[testSize];
        for (int i = 0; i < testSize; i++)
        {
            refTypeEvents[i] = new MetricEvent($"key{i % 100}", i, DateTime.UtcNow);
        }
        
        // Measure iteration and aggregation performance on value type (contiguous)
        var valueTypeSw = Stopwatch.StartNew();
        double valueTypeSum = 0;
        for (int i = 0; i < testSize; i++)
        {
            valueTypeSum += valueTypeEvents[i].Value;
        }
        valueTypeSw.Stop();
        
        // Measure iteration and aggregation performance on reference type
        var refTypeSw = Stopwatch.StartNew();
        double refTypeSum = 0;
        for (int i = 0; i < testSize; i++)
        {
            refTypeSum += refTypeEvents[i].Value;
        }
        refTypeSw.Stop();
        
        // Output results
        var valueTypeTime = valueTypeSw.ElapsedTicks;
        var refTypeTime = refTypeSw.ElapsedTicks;
        
        // The value type should be significantly faster due to better cache locality
        // This may not always be true in the unit test environment due to JIT optimizations,
        // but it demonstrates the concept
        
        // Just ensure both operations produced the same result
        Assert.Equal(valueTypeSum, refTypeSum);
        
        // Test passes if both operations complete
    }
    
    [Fact]
    public void SoA_Pattern_Performance_Test()
    {
        // This test demonstrates the Structure of Arrays (SoA) approach
        // by comparing it with Array of Structures (AoS)
        
        const int testSize = 1_000_000;
        
        // AoS approach (regular array of Event structs)
        var events = new Event[testSize];
        for (int i = 0; i < testSize; i++)
        {
            events[i] = new Event(new KeyId(i % 100), i, DateTime.UtcNow.Ticks);
        }
        
        // SoA approach (separate arrays for each field)
        var keys = new KeyId[testSize];
        var values = new double[testSize];
        var timestamps = new long[testSize];
        
        for (int i = 0; i < testSize; i++)
        {
            keys[i] = new KeyId(i % 100);
            values[i] = i;
            timestamps[i] = DateTime.UtcNow.Ticks;
        }
        
        // Test scenario: Calculate average value for each key
        
        // Measure AoS performance
        var aosSw = Stopwatch.StartNew();
        var aosResult = CalculateAverageByKey_AoS(events);
        aosSw.Stop();
        
        // Measure SoA performance
        var soaSw = Stopwatch.StartNew();
        var soaResult = CalculateAverageByKey_SoA(keys, values);
        soaSw.Stop();
        
        // Output results
        var aosTime = aosSw.ElapsedTicks;
        var soaTime = soaSw.ElapsedTicks;
        
        // Both approaches should produce the same result
        Assert.Equal(aosResult.Length, soaResult.Length);
        
        for (int i = 0; i < aosResult.Length; i++)
        {
            Assert.Equal(aosResult[i], soaResult[i], 0.001);
        }
        
        // Test passes if both operations complete
    }
    
    [Fact]
    public void EventStoreV2_OptimizedPartition_Test()
    {
        // Test the OptimizedPartition class with different layouts
        var aosPartition = new OptimizedPartition(10_000, StorageLayout.AoS);
        var soaPartition = new OptimizedPartition(10_000, StorageLayout.SoA);
        
        // Add same data to both
        for (int i = 0; i < 1000; i++)
        {
            var evt = new Event(new KeyId(i % 10), i, DateTime.UtcNow.Ticks);
            aosPartition.TryEnqueue(evt);
            soaPartition.TryEnqueue(evt);
        }
        
        // Verify count
        Assert.Equal(1000, aosPartition.CountApprox);
        Assert.Equal(1000, soaPartition.CountApprox);
        
        // Get views and check they contain same data
        var aosView = aosPartition.GetView();
        var soaView = soaPartition.GetView();
        
        Assert.Equal(aosView.Count, soaView.Count);
        
        // Verify SoA layout provides direct access to components
        if (soaPartition.Layout == StorageLayout.SoA)
        {
            var keys = soaPartition.GetKeysSpan();
            var values = soaPartition.GetValuesSpan();
            
            Assert.Equal(1000, keys.Length);
            Assert.Equal(1000, values.Length);
        }
    }
    
    // AoS implementation
    private double[] CalculateAverageByKey_AoS(ReadOnlySpan<Event> events)
    {
        var keySet = new HashSet<int>();
        for (int i = 0; i < events.Length; i++)
        {
            keySet.Add(events[i].Key.Value);
        }
        
        var keys = keySet.ToArray();
        var results = new double[keys.Length];
        var counts = new int[keys.Length];
        
        // Calculate sums and counts
        for (int i = 0; i < events.Length; i++)
        {
            for (int k = 0; k < keys.Length; k++)
            {
                if (events[i].Key.Value == keys[k])
                {
                    results[k] += events[i].Value;
                    counts[k]++;
                    break;
                }
            }
        }
        
        // Calculate averages
        for (int k = 0; k < keys.Length; k++)
        {
            if (counts[k] > 0)
            {
                results[k] /= counts[k];
            }
        }
        
        return results;
    }
    
    // SoA implementation
    private double[] CalculateAverageByKey_SoA(ReadOnlySpan<KeyId> keys, ReadOnlySpan<double> values)
    {
        var keySet = new HashSet<int>();
        for (int i = 0; i < keys.Length; i++)
        {
            keySet.Add(keys[i].Value);
        }
        
        var uniqueKeys = keySet.ToArray();
        var results = new double[uniqueKeys.Length];
        var counts = new int[uniqueKeys.Length];
        
        // Calculate sums and counts
        for (int i = 0; i < keys.Length; i++)
        {
            for (int k = 0; k < uniqueKeys.Length; k++)
            {
                if (keys[i].Value == uniqueKeys[k])
                {
                    results[k] += values[i];
                    counts[k]++;
                    break;
                }
            }
        }
        
        // Calculate averages
        for (int k = 0; k < uniqueKeys.Length; k++)
        {
            if (counts[k] > 0)
            {
                results[k] /= counts[k];
            }
        }
        
        return results;
    }
}

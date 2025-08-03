using System;
using Xunit;

namespace LockFree.EventStore.Tests;

/// <summary>
/// Simple integration test for false sharing protection.
/// </summary>
public class IntegrationTest
{
    [Fact]
    public void FalseSharingProtection_BasicFunctionality_ShouldWork()
    {
        // Test with padding enabled
        var paddedOptions = new EventStoreOptions<Event>
        {
            Capacity = 100,
            Partitions = 4,
            EnableFalseSharingProtection = true
        };
        
        var paddedStore = new EventStore<Event>(paddedOptions);
        
        // Basic assertions
        Assert.Equal(4, paddedStore.Partitions);
        Assert.Equal(100, paddedStore.Capacity);
        Assert.True(paddedStore.IsEmpty);
          // Add some events
        for (int i = 0; i < 5; i++)
        {
            var evt = new Event(new KeyId(i), i * 10.0, DateTime.UtcNow.Ticks);
            Assert.True(paddedStore.TryAppend(evt));
        }
        
        Assert.Equal(5, paddedStore.CountApprox);
        Assert.False(paddedStore.IsEmpty);
        
        // Test zero-allocation snapshot
        int totalProcessed = 0;
        paddedStore.SnapshotZeroAlloc(events =>
        {
            totalProcessed += events.Length;
        });
        
        Assert.Equal(5, totalProcessed);
        
        // Test without padding for comparison
        var standardOptions = new EventStoreOptions<Event>
        {
            Capacity = 100,
            Partitions = 4,
            EnableFalseSharingProtection = false // Default behavior
        };
        
        var standardStore = new EventStore<Event>(standardOptions);
        
        // Should have identical API behavior
        Assert.Equal(4, standardStore.Partitions);
        Assert.Equal(100, standardStore.Capacity);
        Assert.True(standardStore.IsEmpty);
          // Add same events
        for (int i = 0; i < 5; i++)
        {
            var evt = new Event(new KeyId(i), i * 10.0, DateTime.UtcNow.Ticks);
            Assert.True(standardStore.TryAppend(evt));
        }
        
        Assert.Equal(5, standardStore.CountApprox);
        
        // Zero-allocation should work identically
        totalProcessed = 0;
        standardStore.SnapshotZeroAlloc(events =>
        {
            totalProcessed += events.Length;
        });
        
        Assert.Equal(5, totalProcessed);
    }
}

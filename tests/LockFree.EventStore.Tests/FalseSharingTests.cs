using System;
using System.Linq;
using Xunit;

namespace LockFree.EventStore.Tests;

/// <summary>
/// Integration tests for false sharing protection functionality.
/// </summary>
public class FalseSharingTests
{
    [Fact]
    public void EventStore_WithFalseSharingProtection_ShouldWork()
    {
        // Arrange
        var options = new EventStoreOptions<Event>
        {
            Capacity = 1000,
            Partitions = 4,
            EnableFalseSharingProtection = true
        };
        
        // Act
        var store = new EventStore<Event>(options);
          // Assert - Basic functionality should work
        Assert.Equal(4, store.Partitions);
        Assert.Equal(1000, store.Capacity);
        Assert.True(store.IsEmpty);
        Assert.False(store.IsFull);
        
        // Test basic append
        var evt = new Event(new KeyId(1), 42.0, DateTime.UtcNow.Ticks);
        Assert.True(store.TryAppend(evt));
        Assert.False(store.IsEmpty);
        Assert.Equal(1, store.CountApprox);
        
        // Test snapshot
        var snapshot = store.Snapshot();
        Assert.Single(snapshot);
        Assert.Equal("KeyId(1)", snapshot[0].Key.ToString());
    }
    
    [Fact]
    public void EventStore_WithoutFalseSharingProtection_ShouldWork()
    {
        // Arrange
        var options = new EventStoreOptions<Event>
        {
            Capacity = 1000,
            Partitions = 4,
            EnableFalseSharingProtection = false
        };
        
        // Act
        var store = new EventStore<Event>(options);
        
        // Assert - Basic functionality should work identically
        Assert.Equal(4, store.Partitions);
        Assert.Equal(1000, store.Capacity);
        Assert.True(store.IsEmpty);
        Assert.False(store.IsFull);
          // Test basic append
        var evt = new Event(new KeyId(2), 42.0, DateTime.UtcNow.Ticks);
        Assert.True(store.TryAppend(evt));
        Assert.False(store.IsEmpty);
        Assert.Equal(1, store.CountApprox);
        
        // Test snapshot
        var snapshot = store.Snapshot();
        Assert.Single(snapshot);
        Assert.Equal("KeyId(2)", snapshot[0].Key.ToString());
    }
    
    [Fact]
    public void EventStore_DefaultOptions_ShouldNotUsePadding()
    {
        // Arrange & Act
        var store = new EventStore<Event>();
        
        // Assert - Should work with default configuration
        Assert.True(store.Partitions > 0);
        Assert.True(store.Capacity > 0);
        Assert.True(store.IsEmpty);
          // Test basic functionality
        var evt = new Event(new KeyId(3), 42.0, DateTime.UtcNow.Ticks);
        Assert.True(store.TryAppend(evt));
        Assert.Equal(1, store.CountApprox);
    }
    
    [Fact]
    public void EventStore_WithPadding_ShouldSupportZeroAllocationMethods()
    {
        // Arrange
        var options = new EventStoreOptions<Event>
        {
            Capacity = 1000,
            Partitions = 4,
            EnableFalseSharingProtection = true
        };
        var store = new EventStore<Event>(options);
          // Add some test data
        for (int i = 0; i < 10; i++)
        {
            store.TryAppend(new Event(new KeyId(i + 10), i * 2.0, DateTime.UtcNow.Ticks + i));
        }
        
        // Act & Assert - Zero allocation methods should work
        int eventCount = 0;
        store.SnapshotZeroAlloc(events =>
        {
            eventCount += events.Length;
        });
        
        Assert.Equal(10, eventCount);
          // Test filtered zero allocation
        int filteredCount = 0;
        store.SnapshotFilteredZeroAlloc(
            evt => evt.Key.Value >= 10,
            events => filteredCount += events.Length);
        
        Assert.Equal(10, filteredCount);
    }
}

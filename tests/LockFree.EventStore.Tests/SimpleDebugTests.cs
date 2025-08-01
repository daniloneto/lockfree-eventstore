using System;
using System.Collections.Generic;
using Xunit;
using LockFree.EventStore;

namespace LockFree.EventStore.Tests;

public sealed class SimpleDebugTests
{
    [Fact]
    public void Debug_TryAppend_CountsCorrectly()
    {
        var store = new EventStore<Order>();
        
        // Initial state
        Assert.Equal(0, store.Statistics.TotalAppended);
        
        // Add first event
        var result1 = store.TryAppend(new Order(1, 10m, DateTime.UtcNow));
        Assert.True(result1);
        Assert.Equal(1, store.Statistics.TotalAppended);
        
        // Add second event
        var result2 = store.TryAppend(new Order(2, 20m, DateTime.UtcNow));
        Assert.True(result2);
        Assert.Equal(2, store.Statistics.TotalAppended);
    }
    
    [Fact]
    public void Debug_TryAppend_WithPartition_CountsCorrectly()
    {
        var store = new EventStore<Order>();
        
        // Initial state
        Assert.Equal(0, store.Statistics.TotalAppended);
        
        // Add first event using partition method
        var result1 = store.TryAppend(new Order(1, 10m, DateTime.UtcNow), 0);
        Assert.True(result1);
        Assert.Equal(1, store.Statistics.TotalAppended);
        
        // Add second event using partition method
        var result2 = store.TryAppend(new Order(2, 20m, DateTime.UtcNow), 0);
        Assert.True(result2);
        Assert.Equal(2, store.Statistics.TotalAppended);
    }
}

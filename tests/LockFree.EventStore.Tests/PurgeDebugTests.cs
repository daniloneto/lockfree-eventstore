using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using LockFree.EventStore;

namespace LockFree.EventStore.Tests;

public sealed class PurgeDebugTests
{
    [Fact]
    public void Debug_Purge_Step_By_Step()
    {
        var store = new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 100
        });

        var now = DateTime.UtcNow;
        var old = now.AddHours(-2);     // Should be purged
        var recent1 = now.AddMinutes(-30); // Should remain
        var recent2 = now.AddMinutes(-10); // Should remain
        var cutoff = now.AddHours(-1);

        // Add events
        store.TryAppend(new Order(1, 10m, old));
        store.TryAppend(new Order(2, 20m, recent1));
        store.TryAppend(new Order(3, 30m, recent2));

        // Check initial state
        Assert.Equal(3, store.Count);
        
        // Check timestamps before purge
        var beforePurge = store.Snapshot();
        Assert.Equal(3, beforePurge.Count);
        
        // Print timestamps for debugging
        Console.WriteLine($"Cutoff: {cutoff}");
        foreach (var evt in beforePurge)
        {
            Console.WriteLine($"Event {evt.Id}: {evt.Timestamp} (keep: {evt.Timestamp >= cutoff})");
        }

        // Purge
        store.Purge(olderThan: cutoff);

        // Check final state
        var afterPurge = store.Snapshot();
        Assert.Equal(2, afterPurge.Count);
        
        // Verify remaining events
        Assert.All(afterPurge, evt => Assert.True(evt.Timestamp >= cutoff));
    }
}

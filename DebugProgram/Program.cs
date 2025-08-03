using System;
using System.Linq;
using LockFree.EventStore;
using LockFree.EventStore.Tests;

namespace DebugProgram;

class Program
{
    static void Main()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 50,
            Partitions = 4
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add events that will be distributed across partitions
        for (int i = 0; i < 8; i++)
        {
            store.TryAppend(new Order(i, (i + 1) * 10m, now.AddMinutes(-i)));
        }

        var windowStart = now.AddMinutes(-10);
        var windowEnd = now;
        
        Console.WriteLine($"Window: from {windowStart:HH:mm:ss.fff} to {windowEnd:HH:mm:ss.fff}");
        Console.WriteLine();
        
        // Check what Query returns
        var queryEvents = store.Query(from: windowStart, to: windowEnd).ToList();
        Console.WriteLine($"Query results: {queryEvents.Count} events");
        var querySum = queryEvents.Sum(o => (double)o.Amount);
        Console.WriteLine($"Query sum: {querySum}");
        foreach (var evt in queryEvents.OrderBy(o => o.Id))
        {
            Console.WriteLine($"  Order {evt.Id}: Amount={evt.Amount}, Time={evt.Timestamp:HH:mm:ss.fff}");
        }
        Console.WriteLine();
        
        // Check what AggregateWindow returns  
        var windowResult = store.AggregateWindow(from: windowStart, to: windowEnd);
        Console.WriteLine($"AggregateWindow results: Count={windowResult.Count}, Sum={windowResult.Sum}");
        
        // Check all events
        Console.WriteLine();
        Console.WriteLine("All events:");
        foreach (var evt in store.EnumerateSnapshot().OrderBy(o => o.Id))
        {
            var inWindow = evt.Timestamp >= windowStart && evt.Timestamp < windowEnd;
            Console.WriteLine($"  Order {evt.Id}: Amount={evt.Amount}, Time={evt.Timestamp:HH:mm:ss.fff}, InWindow={inWindow}");
        }
    }
}

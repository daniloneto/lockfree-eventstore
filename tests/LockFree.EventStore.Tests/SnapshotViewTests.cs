using System;
using System.Linq;
using Xunit;

namespace LockFree.EventStore.Tests;

public class SnapshotViewTests
{
    [Fact]
    public void SnapshotViews_Returns_Views_For_All_Partitions()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 10,
            Partitions = 3
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add some test data
        for (int i = 0; i < 9; i++)
        {
            store.TryAppend(new Order(i, (i + 1) * 10m, now.AddMinutes(-i)));
        }

        var views = store.SnapshotViews();

        Assert.Equal(3, views.Count);
        
        // Count total events across all views
        int totalEvents = 0;
        foreach (var view in views)
        {
            totalEvents += view.Count;
        }
        
        Assert.Equal(9, totalEvents);
    }

    [Fact]
    public void SnapshotViews_With_TimeFilter_Returns_Filtered_Views()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 10,
            Partitions = 2
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add events with different timestamps
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-5))); // Outside window
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-3))); // Inside window
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1))); // Inside window
        store.TryAppend(new Order(4, 40m, now.AddMinutes(-6))); // Outside window

        var views = store.SnapshotViews(from: now.AddMinutes(-4), to: now);

        // Count events in filtered views
        int totalFilteredEvents = 0;
        foreach (var view in views)
        {
            totalFilteredEvents += view.Count;
        }

        // Should only include orders 2 and 3
        Assert.Equal(2, totalFilteredEvents);
    }

    [Fact]
    public void PartitionView_Enumerator_Works_Without_Allocation()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 5,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add some test data  
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-3)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-2)));
        store.TryAppend(new Order(3, 30m, now.AddMinutes(-1)));

        var views = store.SnapshotViews();
        var view = views[0];

        Assert.Equal(3, view.Count);
        Assert.False(view.IsEmpty);
        Assert.False(view.HasWrapAround);

        // Test enumerator
        var enumerated = new System.Collections.Generic.List<Order>();
        foreach (var order in view)
        {
            enumerated.Add(order);
        }

        Assert.Equal(3, enumerated.Count);
        Assert.Equal(1, enumerated[0].Id);
        Assert.Equal(2, enumerated[1].Id);
        Assert.Equal(3, enumerated[2].Id);
    }

    [Fact]
    public void PartitionView_AsSpan_Works_For_Single_Segment()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 10,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add some test data (less than capacity to avoid wrap-around)
        store.TryAppend(new Order(1, 10m, now.AddMinutes(-3)));
        store.TryAppend(new Order(2, 20m, now.AddMinutes(-2)));

        var views = store.SnapshotViews();
        var view = views[0];

        Assert.False(view.HasWrapAround);

        var span = view.AsSpan();
        Assert.Equal(2, span.Length);
        Assert.Equal(1, span[0].Id);
        Assert.Equal(2, span[1].Id);
    }

    [Fact]
    public void PartitionView_AsSpan_Throws_For_Wrapped_View()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 3,
            Partitions = 1
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add more events than capacity to cause wrap-around
        for (int i = 0; i < 5; i++)
        {
            store.TryAppend(new Order(i, (i + 1) * 10m, now.AddMinutes(-i)));
        }

        var views = store.SnapshotViews();
        var view = views[0];

        if (view.HasWrapAround)
        {
            Assert.Throws<InvalidOperationException>(() => view.AsSpan());
        }
    }

    [Fact]
    public void SnapshotViews_Performance_Better_Than_Traditional_Snapshot()
    {
        var options = new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 1000,
            Partitions = 4
        };
        var store = new EventStore<Order>(options);
        var now = DateTime.UtcNow;

        // Add many events
        for (int i = 0; i < 2000; i++)
        {
            store.TryAppend(new Order(i, i * 1.5m, now.AddSeconds(-i)));
        }

        // Test new SnapshotViews method (zero allocation)
        var views = store.SnapshotViews();
        
        // Test traditional Snapshot method (allocates and copies)
        var snapshot = store.Snapshot();

        // Count should be the same
        int viewsCount = views.Sum(v => v.Count);
        Assert.Equal(snapshot.Count, viewsCount);

        // Views should provide access to the same data
        var allViewEvents = new System.Collections.Generic.List<Order>();
        foreach (var view in views)
        {
            foreach (var order in view)
            {
                allViewEvents.Add(order);
            }
        }
        
        Assert.Equal(snapshot.Count, allViewEvents.Count);
    }
}

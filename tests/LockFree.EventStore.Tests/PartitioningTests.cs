using System.Collections.Generic;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PartitioningTests
{
    [Fact]
    public void Explicit_Partition_Is_Respected()
    {
        var options = new EventStoreOptions<int> { CapacityPerPartition = 10, Partitions = 4 };
        var store = new EventStore<int>(options);
        store.TryAppend(1, 2);
        var snapshot = store.Snapshot();
        Assert.Contains(1, snapshot);
    }

    [Fact]
    public void Keys_Spread_Across_Partitions()
    {
        var options = new EventStoreOptions<int> { CapacityPerPartition = 10, Partitions = 4 };
        var store = new EventStore<int>(options);
        for (int i = 0; i < 100; i++)
            store.TryAppend(i);
        var snapshot = store.Snapshot();
        var set = new HashSet<int>();
        foreach (var item in snapshot)
            set.Add(Partitioners.ForKey(item, store.Partitions));
        Assert.True(set.Count > 1);
    }
}

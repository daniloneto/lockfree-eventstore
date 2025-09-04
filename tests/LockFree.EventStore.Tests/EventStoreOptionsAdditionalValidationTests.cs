using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStoreOptionsAdditionalValidationTests
{
    [Fact]
    public void Validate_Throws_When_StatsUpdateInterval_NonPositive()
    {
        var opts = new EventStoreOptions<Order>
        {
            StatsUpdateInterval = 0
        };
        Assert.Throws<InvalidOperationException>(() => opts.Validate());
    }

    [Fact]
    public void GetTotalCapacity_Uses_Explicit_Total_When_Set()
    {
        var opts = new EventStoreOptions<Order>
        {
            CapacityPerPartition = 10,
            Partitions = 4,
            Capacity = 15 // overrides 10*4 = 40
        };
        Assert.Equal(15, opts.GetTotalCapacity());
    }

    [Fact]
    public void GetTotalCapacity_Computed_When_Total_Not_Set()
    {
        var opts = new EventStoreOptions<Order>
        {
            CapacityPerPartition = 10,
            Partitions = 3
        };
        Assert.Equal(30, opts.GetTotalCapacity());
    }
}

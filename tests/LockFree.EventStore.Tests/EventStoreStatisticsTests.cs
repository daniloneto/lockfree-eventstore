using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStoreStatisticsTests
{
    [Fact]
    public void RecordAppend_UpdatesTotals_And_LastAppendTime()
    {
        var stats = new EventStoreStatistics();
        Assert.Equal(0, stats.TotalAppended);
        Assert.Equal(0, stats.TotalDiscarded);
        Assert.Equal(default, stats.LastAppendTime);

        stats.RecordAppend();

        Assert.Equal(1, stats.TotalAppended);
        Assert.Equal(0, stats.TotalDiscarded);
        Assert.NotEqual(default, stats.LastAppendTime);

        // Immediately after append, elapsed < 1s, should return total appended
        Assert.True(stats.AppendsPerSecond >= 1);
    }

    [Fact]
    public void IncrementTotalAdded_Overloads_Work_And_AppendsPerSecond_Branches_Covered()
    {
        var stats = new EventStoreStatistics();

        // With default LastAppendTime, elapsed >= 1s path is taken and returns 0
        Assert.Equal(0, stats.AppendsPerSecond);

        stats.IncrementTotalAdded();
        Assert.Equal(1, stats.TotalAppended);
        var t1 = stats.LastAppendTime;
        Assert.NotEqual(default, t1);

        stats.IncrementTotalAdded(4);
        Assert.Equal(5, stats.TotalAppended);
        var t2 = stats.LastAppendTime;
        Assert.True(t2 >= t1);

        // Immediately after increment, elapsed < 1s path is taken, returns total appended
        Assert.True(stats.AppendsPerSecond >= 5);
    }

    [Fact]
    public void RecordDiscard_And_IncrementOverwritten_Accumulate_Discarded()
    {
        var stats = new EventStoreStatistics();
        stats.RecordDiscard();
        stats.IncrementOverwritten();
        Assert.Equal(2, stats.TotalDiscarded);
    }

    [Fact]
    public void Reset_Clears_All_Counters_And_Time()
    {
        var stats = new EventStoreStatistics();
        stats.RecordAppend();
        stats.RecordDiscard();
        Assert.True(stats.TotalAppended > 0);
        Assert.True(stats.TotalDiscarded > 0);
        Assert.NotEqual(default, stats.LastAppendTime);

        stats.Reset();

        Assert.Equal(0, stats.TotalAppended);
        Assert.Equal(0, stats.TotalDiscarded);
        Assert.Equal(default, stats.LastAppendTime);
        // With default time, APS uses long-elapsed branch returning 0
        Assert.Equal(0, stats.AppendsPerSecond);
    }
}

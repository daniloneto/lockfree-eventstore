using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PartitionWindowStateTests
{
    [Fact]
    public void Reset_Initializes_AllFields()
    {
        var s = new PartitionWindowState
        {
            WindowStartTicks = 123,
            WindowEndTicks = 456,
            Count = 10,
            Sum = 99.9,
            Min = 1,
            Max = 2,
            WindowHeadIndex = 3
        };

        s.Reset();

        Assert.Equal(0, s.WindowStartTicks);
        Assert.Equal(0, s.WindowEndTicks);
        Assert.Equal(0, s.Count);
        Assert.Equal(0.0, s.Sum);
        Assert.Equal(double.MaxValue, s.Min);
        Assert.Equal(double.MinValue, s.Max);
        Assert.Equal(0, s.WindowHeadIndex);
        Assert.Equal(0.0, s.Average);
    }

    [Fact]
    public void Add_Remove_Average_Work()
    {
        var s = new PartitionWindowState();
        s.Reset();

        s.AddValue(10);
        s.AddValue(5);
        Assert.Equal(2, s.Count);
        Assert.Equal(15.0, s.Sum);
        Assert.Equal(7.5, s.Average);
        Assert.Equal(5.0, s.Min);
        Assert.Equal(10.0, s.Max);

        s.RemoveValue(10);
        Assert.Equal(1, s.Count);
        Assert.Equal(5.0, s.Sum);
        Assert.Equal(5.0, s.Average);
        // Min/Max recalculation happens during advance; they remain as last computed
        Assert.Equal(5.0, s.Min);
        Assert.Equal(10.0, s.Max);
    }

    [Fact]
    public void WindowAggregateState_Merge_ToResult()
    {
        var a = new WindowAggregateState { Count = 0, Sum = 0, Min = 0, Max = 0 };
        var b = new WindowAggregateState { Count = 3, Sum = 30, Min = 2, Max = 20 };
        a.Merge(b);
        var r = a.ToResult();
        Assert.Equal(3, r.Count);
        Assert.Equal(30.0, r.Sum);
        Assert.Equal(2.0, r.Min);
        Assert.Equal(20.0, r.Max);
        Assert.Equal(10.0, r.Avg);

        var c = new WindowAggregateState { Count = 2, Sum = 10, Min = 4, Max = 8 };
        a.Merge(c); // total Count=5, Sum=40, Min should be 2, Max 20
        r = a.ToResult();
        Assert.Equal(5, r.Count);
        Assert.Equal(40.0, r.Sum);
        Assert.Equal(2.0, r.Min);
        Assert.Equal(20.0, r.Max);
        Assert.Equal(8.0, Math.Round(r.Avg, 6));
    }

    [Fact]
    public void WindowRemoveState_Default_And_Ctor()
    {
        var d = new WindowRemoveState();
        Assert.Equal(0, d.RemovedCount);
        var e = new WindowRemoveState(5);
        Assert.Equal(5, e.RemovedCount);
    }
}

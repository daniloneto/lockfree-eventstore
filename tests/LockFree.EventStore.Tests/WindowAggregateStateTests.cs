using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class WindowAggregateStateTests
{
    [Fact]
    public void Merge_First_Assigns_Min_Max_Afterwards_Updates()
    {
        var a = new WindowAggregateState { Count = 2, Sum = 30, Min = 10, Max = 20 };
        var b = new WindowAggregateState { Count = 3, Sum = 90, Min = 15, Max = 40 };

        // Start with empty accumulator
        var acc = new WindowAggregateState();
        acc.Merge(a); // first merge copies min/max from a
        Assert.Equal(2, acc.Count);
        Assert.Equal(30, acc.Sum);
        Assert.Equal(10, acc.Min);
        Assert.Equal(20, acc.Max);

        acc.Merge(b); // second merge updates aggregates
        Assert.Equal(5, acc.Count);
        Assert.Equal(120, acc.Sum);
        Assert.Equal(10, acc.Min); // smaller of 10 and 15
        Assert.Equal(40, acc.Max); // larger of 20 and 40

        var result = acc.ToResult();
        Assert.Equal(5, result.Count);
        Assert.Equal(120, result.Sum, 5);
        Assert.Equal(10, result.Min, 5);
        Assert.Equal(40, result.Max, 5);
        Assert.Equal(24, result.Avg, 5);
    }

    [Fact]
    public void ToResult_Empty_Returns_Zeros()
    {
        var s = new WindowAggregateState();
        var r = s.ToResult();
        Assert.Equal(0, r.Count);
        Assert.Equal(0, r.Sum, 5);
        Assert.Equal(0, r.Min, 5);
        Assert.Equal(0, r.Max, 5);
        Assert.Equal(0, r.Avg, 5);
    }
}
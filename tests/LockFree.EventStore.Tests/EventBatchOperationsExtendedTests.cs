using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventBatchOperationsExtendedTests
{
    private static Event E(int key, double value, long ticks) => new(new KeyId(key), value, ticks);

    [Fact]
    public void SumByKey_Works_For_Matching_And_NonMatching()
    {
        var events = new[]
        {
            E(1, 10, 100), E(2, 5, 110), E(1, 7, 120), E(3, 1, 130)
        };
        var sum1 = EventBatchOperations.SumByKey(events, new KeyId(1));
        var sum2 = EventBatchOperations.SumByKey(events, new KeyId(2));
        var sum4 = EventBatchOperations.SumByKey(events, new KeyId(4));
        Assert.Equal(17, sum1);
        Assert.Equal(5, sum2);
        Assert.Equal(0, sum4);
    }

    [Fact]
    public void AverageByKey_Computes_Average_Or_Zero()
    {
        var events = new[]
        {
            E(5, 2, 100), E(5, 4, 110), E(6, 10, 120)
        };
        var avg5 = EventBatchOperations.AverageByKey(events, new KeyId(5));
        var avg6 = EventBatchOperations.AverageByKey(events, new KeyId(6));
        var avg7 = EventBatchOperations.AverageByKey(events, new KeyId(7));
        Assert.Equal(3, avg5);
        Assert.Equal(10, avg6);
        Assert.Equal(0, avg7);
    }

    [Fact]
    public void FilterByTimeRange_Honors_Output_Capacity_And_Range()
    {
        var events = new[]
        {
            E(1, 1, 100), E(1, 2, 150), E(1, 3, 200), E(1, 4, 250)
        };
        Span<Event> outBuf = stackalloc Event[2];
        var count = EventBatchOperations.FilterByTimeRange(events, 120, 240, outBuf);
        Assert.Equal(2, count);
        Assert.Equal(150, outBuf[0].TimestampTicks);
        Assert.Equal(200, outBuf[1].TimestampTicks);
    }

    [Fact]
    public void GetTimeRange_Returns_Empty_And_Valid_Ranges()
    {
        var (f1, l1) = EventBatchOperations.GetTimeRange(ReadOnlySpan<Event>.Empty);
        Assert.Equal(0, f1);
        Assert.Equal(0, l1);

        var events = new[] { E(1, 1, 10), E(1, 1, 20), E(1, 1, 30) };
        var (f2, l2) = EventBatchOperations.GetTimeRange(events);
        Assert.Equal(10, f2);
        Assert.Equal(30, l2);
    }

    [Fact]
    public void AggregateByKeys_Handles_Empty_NoMatches_And_Partial()
    {
        Func<ReadOnlySpan<double>, double> sumAgg = span =>
        {
            double s = 0; for (int i = 0; i < span.Length; i++) s += span[i]; return s;
        };
        var empty = EventBatchOperations.AggregateByKeys(ReadOnlySpan<Event>.Empty, ReadOnlySpan<KeyId>.Empty, sumAgg);
        Assert.NotNull(empty);
        Assert.Empty(empty);

        var events = new[] { E(1, 1, 1), E(2, 2, 2), E(1, 3, 3) };
        var keys = new[] { new KeyId(3), new KeyId(2), new KeyId(1) };
        var res = EventBatchOperations.AggregateByKeys(events, keys, sumAgg);
        Assert.Equal(3, res.Length);
        Assert.Equal(0, res[0]); // no matches for key 3
        Assert.Equal(2, res[1]); // single
        Assert.Equal(4, res[2]); // 1 + 3
    }
}

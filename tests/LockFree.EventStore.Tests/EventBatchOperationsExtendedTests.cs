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
        const double tol = 1e-9;
        Assert.InRange(avg5, 3 - tol, 3 + tol);
        Assert.InRange(avg6, 10 - tol, 10 + tol);
        Assert.InRange(avg7, 0 - tol, 0 + tol);
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
    public void FilterByTimeRange_Clamps_To_Capacity_And_Includes_Boundaries()
    {
        var events = new[]
        {
            E(1, 0, 50), E(1, 0, 100), E(1, 0, 120), E(1, 0, 150), E(1, 0, 180), E(1, 0, 200), E(1, 0, 240), E(1, 0, 260)
        };
        var from = 100L; var to = 240L; // inclusive bounds expected

        // Capacity clamp: more matches (6) than buffer (3) -> only first 3 in input order
        Span<Event> small = stackalloc Event[3];
        var writtenSmall = EventBatchOperations.FilterByTimeRange(events, from, to, small);
        Assert.Equal(3, writtenSmall);
        Assert.Equal(100, small[0].TimestampTicks);
        Assert.Equal(120, small[1].TimestampTicks);
        Assert.Equal(150, small[2].TimestampTicks);

        // Sufficient capacity: should include both boundaries (100 and 240) and preserve order
        Span<Event> large = stackalloc Event[10];
        var writtenLarge = EventBatchOperations.FilterByTimeRange(events, from, to, large);
        Assert.Equal(6, writtenLarge);
        Assert.Equal(new long[] { 100, 120, 150, 180, 200, 240 }, large.Slice(0, writtenLarge).ToArray().Select(e => e.TimestampTicks));
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
    public void GetTimeRange_Unsorted_Uses_First_And_Last_Not_MinMax()
    {
        // Current contract returns first/last without sorting; this test locks in that behavior.
        var unsorted = new[] { E(1, 0, 30), E(1, 0, 10), E(1, 0, 20) };
        var (first, last) = EventBatchOperations.GetTimeRange(unsorted);
        Assert.Equal(30, first);
        Assert.Equal(20, last);
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

    [Fact]
    public void AggregateByKeys_DuplicateKeys_And_EmptyKeysWithEvents()
    {
        var events = new[] { E(1, 1, 1), E(2, 2, 2), E(1, 3, 3) }; // key 1 appears twice -> sum 4
        Func<ReadOnlySpan<double>, double> sumAgg = span =>
        {
            double s = 0; for (int i = 0; i < span.Length; i++) s += span[i]; return s;
        };

        // Duplicate keys: implementation assigns event to the first matching key index only
        var keysWithDupes = new[] { new KeyId(1), new KeyId(1), new KeyId(2), new KeyId(3) };
        var resDupes = EventBatchOperations.AggregateByKeys(events, keysWithDupes, sumAgg);
        Assert.Equal(new double[] { 4, 0, 2, 0 }, resDupes);

        // Empty keys with existing events should return an empty result
        var emptyKeys = ReadOnlySpan<KeyId>.Empty;
        var resEmptyKeys = EventBatchOperations.AggregateByKeys(events, emptyKeys, sumAgg);
        Assert.Empty(resEmptyKeys);
    }
}

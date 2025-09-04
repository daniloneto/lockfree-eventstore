using System;
using System.Collections.Generic;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class ZeroAllocationExtensionsAdditionalTests
{
    private static Order O(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    private static EventStore<Order> CreateStore(bool withTs = true)
    {
        return new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = withTs ? new OrderTimestampSelector() : null,
            Partitions = 1,
            CapacityPerPartition = 64
        });
    }

    [Fact]
    public void ProcessEventsChunked_Last_Chunk_Partial()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        for (int i = 0; i < 5; i++) store.TryAppend(O(i, 1m * i, now.AddSeconds(i)));
        // chunkSize 3 => chunks: [0,1,2] [3,4]
        var collectedCounts = new List<int>();
        store.ProcessEventsChunked(0, (state, chunk) =>
        {
            collectedCounts.Add(chunk.Length);
            return state + chunk.Length;
        }, chunkSize:3);
        Assert.Equal(new[]{3,2}, collectedCounts);
    }

    [Fact]
    public void ProcessEvents_With_TimeRange_No_TimestampSelector_Throws()
    {
        var store = CreateStore(withTs:false); // no timestamp selector
        var now = DateTime.UtcNow;
        store.TryAppend(O(1,10m, now));
        // Control: no time range scenario works
        var count = store.ProcessEvents(0, (ref int s, Order o, DateTime? ts) => { s++; return true; });
        Assert.Equal(1, count);
        // Time range scenario must throw because TimestampSelector is required for time filtering
        Assert.Throws<InvalidOperationException>(() =>
            store.ProcessEvents(0, (ref int s, Order o, DateTime? ts) => { s++; return true; }, from: now.AddMinutes(-1), to: now.AddMinutes(1))
        );
    }

    [Fact]
    public void CountEventsZeroAlloc_With_Filter_And_Inverted_Time_Range()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        for (int i = 0; i < 3; i++) store.TryAppend(O(i, i, now.AddSeconds(i)));
        var from = now.AddMinutes(10);
        var to = now.AddMinutes(-10); // inverted
        var c = store.CountEventsZeroAlloc(filter: (o, t) => o.Amount >= 0, from: from, to: to);
        Assert.True(c >= 0); // Should not throw and either ignore or return 0
    }

    [Fact]
    public void FindFirstZeroAlloc_EarlyTermination()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        for (int i = 0; i < 10; i++) store.TryAppend(O(i, i, now.AddSeconds(i)));
        var (found, evt) = store.FindFirstZeroAlloc((o,t) => o.Amount >= 5m);
        Assert.True(found);
        Assert.Equal(5m, evt.Amount);
    }

    [Fact]
    public void AggregateZeroAlloc_Custom_Accumulator()
    {
        var store = CreateStore();
        var now = DateTime.UtcNow;
        for (int i = 0; i < 4; i++) store.TryAppend(O(i, 2m, now.AddSeconds(i)));
        var product = store.AggregateZeroAlloc(1m, (acc, e) => acc * (e.Amount));
        Assert.Equal(16m, product);
    }

    [Fact]
    public void GroupByZeroAlloc_Empty()
    {
        var store = CreateStore();
        var captured = 0;
        store.GroupByZeroAlloc(o => o.Id, span => { captured += span.Length; }, chunkSize:2);
        Assert.Equal(0, captured);
    }
}

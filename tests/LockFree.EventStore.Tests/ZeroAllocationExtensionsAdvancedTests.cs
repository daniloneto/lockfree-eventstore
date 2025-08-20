using System;
using System.Collections.Generic;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class ZeroAllocationExtensionsAdvancedTests
{
    private static Order O(int id, decimal amount, DateTime ts) => new(id, amount, ts);

    private static EventStore<Order> CreateOrderStore()
    {
        return new EventStore<Order>(new EventStoreOptions<Order>
        {
            TimestampSelector = new OrderTimestampSelector(),
            CapacityPerPartition = 128,
            Partitions = 2
        });
    }

    [Fact]
    public void Sum_Min_Max_Average_ZeroAlloc_Work_With_And_Without_Filter()
    {
        var store = CreateOrderStore();
        var now = DateTime.UtcNow;
        store.TryAppend(O(1, 10m, now.AddMinutes(-10)));
        store.TryAppend(O(2, 20m, now.AddMinutes(-5)));
        store.TryAppend(O(3, 30m, now.AddMinutes(-1)));

        double sumAll = store.SumZeroAlloc(o => (double)o.Amount);
        Assert.Equal(60d, sumAll);

        double sumFiltered = store.SumZeroAlloc(o => (double)o.Amount, filter: (o, t) => o.Amount >= 20m);
        Assert.Equal(50d, sumFiltered);

        var min = store.MinZeroAlloc(o => o.Amount);
        Assert.True(min.HasValue);
        Assert.Equal(10m, min!.Value);

        var max = store.MaxZeroAlloc(o => o.Amount);
        Assert.True(max.HasValue);
        Assert.Equal(30m, max!.Value);

        double avg = store.AverageZeroAlloc(o => (double)o.Amount);
        Assert.Equal(20d, avg);

        double avgFiltered = store.AverageZeroAlloc(o => (double)o.Amount, filter: (o, t) => o.Amount > 10m);
        Assert.Equal(25d, avgFiltered);
    }

    [Fact]
    public void GroupByZeroAlloc_Groups_In_Chunks()
    {
        var store = CreateOrderStore();
        var now = DateTime.UtcNow;
        for (int i = 0; i < 10; i++)
        {
            store.TryAppend(O(i % 3, i, now.AddSeconds(i)));
        }

        var collected = new List<KeyValuePair<int, List<Order>>>();
        store.GroupByZeroAlloc(o => o.Id % 3, span =>
        {
            collected.AddRange(span.ToArray());
        }, chunkSize: 2);

        // Verify groups and counts
        var dict = collected.ToDictionary(k => k.Key, v => v.Value);
        Assert.Equal(3, dict.Count);
        Assert.Equal(4, dict[0].Count);
        Assert.Equal(3, dict[1].Count);
        Assert.Equal(3, dict[2].Count);
    }

    [Fact]
    public void ProcessEventsChunked_With_Time_Filter_Works()
    {
        var store = CreateOrderStore();
        var now = DateTime.UtcNow;
        store.TryAppend(O(1, 10m, now.AddMinutes(-10)));
        store.TryAppend(O(2, 20m, now.AddMinutes(-3)));
        store.TryAppend(O(3, 30m, now.AddMinutes(-1)));

        var from = now.AddMinutes(-5);
        var to = now;
        var sum = store.ProcessEventsChunked(0m, (acc, chunk) =>
        {
            foreach (var o in chunk) acc += o.Amount;
            return acc;
        }, filter: (o, t) => t >= from && t <= to, from: from, to: to, chunkSize: 2);

        Assert.Equal(50m, sum);
    }
}

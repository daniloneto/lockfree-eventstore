using LockFree.EventStore;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace LockFree.EventStore.Tests;

public class SpecializedEventStoreTests
{
    [Fact]
    public void SpecializedEventStore_AddSingleEvent_Works()
    {
        // Arrange
        var store = new SpecializedEventStore(1024, 4);
        var keyId = new KeyId(1);
        var eventItem = new Event(keyId, 42.5, DateTime.UtcNow.Ticks);

        // Act
        store.Add(eventItem);

        // Assert
        Assert.Equal(1, store.CountApprox);
        Assert.False(store.IsEmpty);
    }

    [Fact]
    public void SpecializedEventStore_AddMultipleEvents_Works()
    {
        // Arrange
        var store = new SpecializedEventStore(1024, 4);
        var events = new Event[]
        {
            new Event(new KeyId(1), 10.0, DateTime.UtcNow.Ticks),
            new Event(new KeyId(2), 20.0, DateTime.UtcNow.Ticks),
            new Event(new KeyId(3), 30.0, DateTime.UtcNow.Ticks)
        };

        // Act
        store.AddRange(events);

        // Assert
        Assert.Equal(3, store.CountApprox);
    }

    [Fact]
    public void SpecializedEventStore_QueryByKey_Works()
    {
        // Arrange
        var store = new SpecializedEventStore(1024, 4);
        var keyId = new KeyId(1);
        var eventItem = new Event(keyId, 42.5, DateTime.UtcNow.Ticks);
        store.Add(eventItem);

        // Act
        var results = new List<Event>();
        store.QueryByKeyZeroAlloc(keyId, span =>
        {
            for (int i = 0; i < span.Length; i++) results.Add(span[i]);
        });

        // Assert
        Assert.Single(results);
        Assert.Equal(keyId, results[0].Key);
        Assert.Equal(42.5, results[0].Value);
    }

    [Fact]
    public void SpecializedEventStore_EnumerateSnapshot_Works()
    {
        // Arrange
        var store = new SpecializedEventStore(1024, 4);
        var events = new Event[]
        {
            new Event(new KeyId(1), 10.0, DateTime.UtcNow.Ticks),
            new Event(new KeyId(2), 20.0, DateTime.UtcNow.Ticks)
        };
        store.AddRange(events);

        // Act
        var snapshot = store.EnumerateSnapshot().ToList();

        // Assert
        Assert.Equal(2, snapshot.Count);
    }

    [Fact]
    public void SpecializedEventStore_AggregateByKey_Works()
    {
        // Arrange
        var store = new SpecializedEventStore(1024, 4);
        var keyId1 = new KeyId(1);
        var keyId2 = new KeyId(2);
        
        store.Add(new Event(keyId1, 10.0, DateTime.UtcNow.Ticks));
        store.Add(new Event(keyId1, 20.0, DateTime.UtcNow.Ticks));
        store.Add(new Event(keyId2, 30.0, DateTime.UtcNow.Ticks));

        // Act
        var aggregates = store.AggregateByKey();

        // Assert
        Assert.Equal(30.0, aggregates[keyId1]); // 10 + 20
        Assert.Equal(30.0, aggregates[keyId2]);
    }

    [Fact]
    public void SpecializedEventStore_GetLatestValues_Works()
    {
        // Arrange
        var store = new SpecializedEventStore(1024, 4);
        var keyId = new KeyId(1);
        var now = DateTime.UtcNow;
        
        store.Add(new Event(keyId, 10.0, now.Ticks));
        Thread.Sleep(1); // Ensure different timestamps
        store.Add(new Event(keyId, 20.0, now.AddMilliseconds(1).Ticks));

        // Act
        var latestValues = store.GetLatestValues();

        // Assert
        Assert.Single(latestValues);
        Assert.Equal(20.0, latestValues[keyId].Value); // Should have latest value
    }

    [Fact]
    public void EventStoreFactory_CreateForEvent_Works()
    {
        // Arrange & Act
        var store = EventStoreFactory.CreateForEvent(1024, 4);

        // Assert
        Assert.NotNull(store);
        Assert.Equal(4, store.Partitions);
        Assert.Equal(1024, store.Capacity);
    }

    [Fact]
    public void EventStoreFactory_CreateOptimized_ForEvent_ReturnsSpecialized()
    {
        // Arrange & Act
        var store = EventStoreFactory.CreateOptimized<Event>(1024, 4);

        // Assert
        Assert.IsType<SpecializedEventStore>(store);
    }

    [Fact]
    public void EventStoreFactory_CreateOptimized_ForInt_ReturnsGeneric()
    {
        // Arrange & Act
        var store = EventStoreFactory.CreateOptimized<int>(1024, 4);

        // Assert
        Assert.IsType<EventStore<int>>(store);
    }
}

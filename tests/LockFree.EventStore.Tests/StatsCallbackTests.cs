using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace LockFree.EventStore.Tests;

public class StatsCallbackTests
{
    [Fact]
    public void OnStatsUpdated_CalledOnAppendCount()
    {
        // Arrange
        var collected = new List<StoreStats>();
        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 100,
            OnStatsUpdated = stats => collected.Add(stats)
        };
        
        var store = new EventStore<int>(options);
        
        // Act
        store.TryAppend(1);
        store.TryAppend(2);
        store.TryAppend(3);
        
        // Assert
        Assert.Equal(3, collected.Count);
        Assert.Equal(1, collected[0].AppendCount);
        Assert.Equal(2, collected[1].AppendCount);
        Assert.Equal(3, collected[2].AppendCount);
        
        // All other counters should be 0 for append operations
        foreach (var stats in collected)
        {
            Assert.Equal(0, stats.DroppedCount);
            Assert.Equal(0, stats.WindowAdvanceCount);
        }
    }    [Fact]
    public void OnStatsUpdated_CalledOnDroppedCount()
    {
        // Arrange
        var collected = new List<StoreStats>();        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 2, // Small size to trigger drops
            OnStatsUpdated = stats => collected.Add(stats)
            // Remove OnEventDiscarded to test that stats still work without it
        };
        
        var store = new EventStore<int>(options);
          // Fill the store to capacity
        store.TryAppend(1);
        store.TryAppend(2);
        
        // Clear collected stats from initial appends
        collected.Clear();        
        
        // Act - this should cause a drop
        store.TryAppend(3);
        
        // Assert
        Assert.True(collected.Count > 0, "No stats callbacks were received");
        var lastStats = collected[^1];
        Assert.True(lastStats.DroppedCount > 0, $"Expected DroppedCount > 0, but was {lastStats.DroppedCount}");
    }
    
    [Fact]
    public void OnStatsUpdated_CalledOnBatchAppend()
    {
        // Arrange
        var collected = new List<StoreStats>();
        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 100,
            OnStatsUpdated = stats => collected.Add(stats)
        };
        
        var store = new EventStore<int>(options);
        var batch = new[] { 1, 2, 3, 4, 5 };
        
        // Act
        var written = store.TryAppendBatch(batch);
        
        // Assert
        Assert.Equal(5, written);
        Assert.True(collected.Count > 0);
        var lastStats = collected[^1];
        Assert.Equal(5, lastStats.AppendCount);
    }
    
    [Fact]
    public void OnStatsUpdated_CalledOnSnapshotBytesExposed()
    {
        // Arrange
        var collected = new List<StoreStats>();
        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 100,
            OnStatsUpdated = stats => collected.Add(stats)
        };
        
        var store = new EventStore<int>(options);
        
        // Add some data first
        store.TryAppend(1);
        store.TryAppend(2);
        
        // Clear initial stats
        collected.Clear();
        
        // Act - this should track bytes exposed
        var snapshot = store.Snapshot();
        
        // Assert
        Assert.True(collected.Count > 0);
        var lastStats = collected[^1];
        Assert.True(lastStats.SnapshotBytesExposed > 0);
    }
    
    [Fact]
    public void OnStatsUpdated_NotCalledWhenNull()
    {
        // Arrange - no callback provided
        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 100
            // OnStatsUpdated is null by default
        };
        
        var store = new EventStore<int>(options);
        
        // Act - should not throw even without callback
        store.TryAppend(1);
        store.TryAppend(2);
        var snapshot = store.Snapshot();
        
        // Assert - no exception should be thrown
        Assert.True(true); // Test passes if no exception
    }
    
    [Fact]
    public void OnStatsUpdated_ExceptionInCallbackDoesNotBreakFlow()
    {
        // Arrange
        var callCount = 0;
        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 100,
            OnStatsUpdated = stats => 
            {
                callCount++;
                throw new InvalidOperationException("Test exception");
            }
        };
        
        var store = new EventStore<int>(options);
        
        // Act - should not throw even when callback throws
        store.TryAppend(1);
        store.TryAppend(2);
        
        // Assert
        Assert.Equal(2, callCount); // Callback was called despite exceptions
        
        // Verify that operations still work
        var snapshot = store.Snapshot();
        Assert.Equal(2, snapshot.Count);
        Assert.Equal(3, callCount); // Snapshot also triggered callback
    }
    
    [Fact]
    public void OnStatsUpdated_ConcurrentAccess()
    {
        // Arrange
        var collected = new List<StoreStats>();
        var lockObj = new object();
        var options = new EventStoreOptions<int>
        {
            Partitions = 4,
            CapacityPerPartition = 100,
            OnStatsUpdated = stats => 
            {
                lock (lockObj)
                {
                    collected.Add(stats);
                }
            }
        };
        
        var store = new EventStore<int>(options);
        
        // Act - concurrent appends
        var tasks = new Task[10];
        for (int i = 0; i < tasks.Length; i++)
        {
            var taskId = i;
            tasks[i] = Task.Run(() =>
            {
                for (int j = 0; j < 10; j++)
                {
                    store.TryAppend(taskId * 10 + j);
                }
            });
        }
        
        Task.WaitAll(tasks);
        
        // Assert
        lock (lockObj)
        {
            Assert.True(collected.Count >= 100); // At least 100 callbacks (one per append)
            var finalStats = collected[^1];
            Assert.Equal(100, finalStats.AppendCount);
        }
    }    [Fact]
    public void OnStatsUpdated_WindowAdvanceCount()
    {
        // Arrange
        var collected = new List<StoreStats>();
        var startTime = DateTime.UtcNow.Ticks;
        var options = new EventStoreOptions<int>
        {
            Partitions = 1,
            CapacityPerPartition = 100,
            WindowSizeTicks = TimeSpan.FromMilliseconds(10).Ticks,
            OnStatsUpdated = stats => collected.Add(stats),
            // Add a timestamp selector that uses the current time
            TimestampSelector = new IntTimestampSelector(startTime)
        };
        
        var store = new EventStore<int>(options);
          // Act
        store.TryAppend(1);
        // Force window advance by simulating time passage (avoid Thread.Sleep)
        // Simule o avanço do tempo ajustando o timestamp do evento ou usando um mock/fake de tempo se possível
        store.TryAppend(2);
        
        // Assert
        var statsWithWindowAdvance = collected.Where(s => s.WindowAdvanceCount > 0).ToList();
        Assert.True(statsWithWindowAdvance.Count > 0, $"Expected at least one stat with WindowAdvanceCount > 0. Total stats: {collected.Count}, WindowAdvance stats: {statsWithWindowAdvance.Count}");
    }
    
    // Helper class for timestamp selection on integers
    private class IntTimestampSelector : IEventTimestampSelector<int>
    {
        private readonly long _baseTime;
        private long _counter = 0;
        
        public IntTimestampSelector(long baseTime)
        {
            _baseTime = baseTime;
        }
        
        public DateTime GetTimestamp(int e)
        {
            // Each integer gets a timestamp that advances by 1ms
            var offset = Interlocked.Increment(ref _counter) * TimeSpan.TicksPerMillisecond;
            return new DateTime(_baseTime + offset);
        }
        
        public long GetTimestampTicks(int e)
        {
            return GetTimestamp(e).Ticks;
        }
    }
}

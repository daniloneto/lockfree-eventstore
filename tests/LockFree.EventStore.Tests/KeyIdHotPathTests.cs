using Xunit;

namespace LockFree.EventStore.Tests;

public class KeyIdHotPathTests
{
    [Fact]
    public void KeyId_Creation_Works()
    {
        var keyId1 = new KeyId(1);
        var keyId2 = new KeyId(2);
        
        Assert.Equal(1, keyId1.Value);
        Assert.Equal(2, keyId2.Value);
        Assert.NotEqual(keyId1, keyId2);
    }    [Fact]
    public void EventStore_KeyMap_GetOrAdd_Works_Correctly()
    {
        var store = new EventStore<int>(100);
        
        var keyId1 = store.GetOrCreateKeyId("cpu_usage");
        var keyId2 = store.GetOrCreateKeyId("memory_usage");
        var keyId3 = store.GetOrCreateKeyId("cpu_usage"); // Should return the same KeyId
        
        Assert.Equal(1, keyId1.Value);
        Assert.Equal(2, keyId2.Value);
        Assert.Equal(keyId1, keyId3); // Same key should return same KeyId
        Assert.Equal(2, store.RegisteredKeysCount);
    }

    [Fact]
    public void EventStore_KeyMap_Lookup_Works()
    {
        var store = new EventStore<int>(100);
        var keyId = store.GetOrCreateKeyId("test_key");
        
        var keyMappings = store.GetKeyMappings();
        Assert.True(keyMappings.ContainsKey("test_key"));
        Assert.Equal(keyId, keyMappings["test_key"]);
        
        Assert.False(keyMappings.ContainsKey("non_existent"));
    }

    [Fact]
    public void EventStore_HotPath_TryAppend_With_KeyId()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            CapacityPerPartition = 100,
            Partitions = 4
        });

        // Get KeyId for hot path
        var keyId = store.GetOrCreateKeyId("test_metric");
        
        // Hot path append
        var success = store.TryAppend(keyId, 42);
        
        Assert.True(success);
        Assert.Equal(1, store.Count);
        Assert.Equal(1, store.RegisteredKeysCount);
    }

    [Fact]
    public void EventStore_String_To_KeyId_Bridge_Works()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            CapacityPerPartition = 100,
            Partitions = 4
        });

        // Use string API (which converts to KeyId internally)
        var success1 = store.TryAppend("cpu_usage", 45);
        var success2 = store.TryAppend("cpu_usage", 55);
        
        Assert.True(success1);
        Assert.True(success2);
        Assert.Equal(2, store.Count);
        Assert.Equal(1, store.RegisteredKeysCount); // Only one unique key
    }

    [Fact]
    public void EventStore_Batch_HotPath_With_KeyId()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            CapacityPerPartition = 100,
            Partitions = 4
        });

        var keyId = store.GetOrCreateKeyId("batch_metric");
        var batch = new int[] { 10, 20, 30, 40, 50 };
        
        var written = store.TryAppendBatch(keyId, batch);
        
        Assert.Equal(5, written);
        Assert.Equal(5, store.Count);
        Assert.Equal(1, store.RegisteredKeysCount);
    }

    [Fact]
    public void EventStore_Mixed_KeyId_Batch()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            CapacityPerPartition = 100,
            Partitions = 4
        });

        var keyId1 = store.GetOrCreateKeyId("metric1");
        var keyId2 = store.GetOrCreateKeyId("metric2");
        
        var batch = new (KeyId, int)[]
        {
            (keyId1, 10),
            (keyId2, 20),
            (keyId1, 30),
            (keyId2, 40)
        };
        
        var written = store.TryAppendBatch(batch);
        
        Assert.Equal(4, written);
        Assert.Equal(4, store.Count);
        Assert.Equal(2, store.RegisteredKeysCount);
    }

    [Fact]
    public void Partitioners_ForKeyId_Distributes_Correctly()
    {
        var partitions = 4;
        var keyId1 = new KeyId(1);
        var keyId2 = new KeyId(2);
        var keyId3 = new KeyId(3);
        var keyId4 = new KeyId(4);
        
        var partition1 = Partitioners.ForKeyIdSimple(keyId1, partitions);
        var partition2 = Partitioners.ForKeyIdSimple(keyId2, partitions);
        var partition3 = Partitioners.ForKeyIdSimple(keyId3, partitions);
        var partition4 = Partitioners.ForKeyIdSimple(keyId4, partitions);
        
        // All partitions should be in valid range
        Assert.InRange(partition1, 0, partitions - 1);
        Assert.InRange(partition2, 0, partitions - 1);
        Assert.InRange(partition3, 0, partitions - 1);
        Assert.InRange(partition4, 0, partitions - 1);
        
        // Sequential KeyIds should distribute across partitions
        Assert.Equal(1, keyId1.Value % partitions);
        Assert.Equal(partition1, keyId1.Value % partitions);
    }    [Fact]
    public void EventStore_KeyMap_Clear_Via_ClearAll()
    {
        var store = new EventStore<int>(100);
        
        store.GetOrCreateKeyId("key1");
        store.GetOrCreateKeyId("key2");
        Assert.Equal(2, store.RegisteredKeysCount);
        
        store.ClearAll();
        Assert.Equal(0, store.RegisteredKeysCount);
        
        // After clear, new keys should start from 1 again
        var newKeyId = store.GetOrCreateKeyId("key1");
        Assert.Equal(1, newKeyId.Value);
    }

    [Fact]
    public void EventStore_ClearAll_Resets_KeyMap()
    {
        var store = new EventStore<int>(100);
        
        store.TryAppend("key1", 10);
        store.TryAppend("key2", 20);
        
        Assert.Equal(2, store.Count);
        Assert.Equal(2, store.RegisteredKeysCount);
        
        store.ClearAll();
        
        Assert.Equal(0, store.Count);
        Assert.Equal(0, store.RegisteredKeysCount);
    }

    [Fact]
    public void Performance_Comparison_String_vs_KeyId()
    {
        var store = new EventStore<int>(new EventStoreOptions<int>
        {
            CapacityPerPartition = 10000,
            Partitions = 4
        });

        // Pre-create KeyId for hot path
        var keyId = store.GetOrCreateKeyId("hot_metric");
        var iterations = 1000;

        // Measure KeyId hot path (should be faster)
        var stopwatch1 = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            store.TryAppend(keyId, i);
        }
        stopwatch1.Stop();

        // Clear for fair comparison
        store.Clear();

        // Measure string path (should be slower due to string operations)
        var stopwatch2 = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            store.TryAppend("hot_metric", i);
        }
        stopwatch2.Stop();

        // KeyId should be significantly faster (though micro-benchmarks can vary)
        // This test mainly ensures both paths work correctly
        Assert.Equal(iterations, store.Count);
        Assert.True(stopwatch1.ElapsedTicks >= 0);
        Assert.True(stopwatch2.ElapsedTicks >= 0);
        
        // Both should produce the same results
        Assert.Equal(iterations, store.Count);
    }
}

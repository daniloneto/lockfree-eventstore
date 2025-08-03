using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Extension methods for the EventStore to provide query capabilities.
/// </summary>
public static class EventStoreQueries
{
    /// <summary>
    /// Queries all events for a specific key across all partitions.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Event> QueryByKey(this EventStoreV2 store, string key)
    {
        var keyId = new KeyMap().GetOrAdd(key);
        return QueryByKeyId(store, keyId);
    }
      /// <summary>
    /// Queries all events for a specific key ID across all partitions.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Event> QueryByKeyId(this EventStoreV2 store, KeyId keyId)
    {
        int partitionIndex = Math.Abs(keyId.GetHashCode()) % store.Partitions;
        var view = store.GetPartitionView(partitionIndex);
        
        // Use a list to collect matching events instead of using yield return with the ref struct enumerator
        var results = new List<Event>();
        foreach (var e in view)
        {
            if (e.Key.Value == keyId.Value)
            {
                results.Add(e);
            }
        }
        
        return results;
    }
    
    /// <summary>
    /// Aggregates values for a specific key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double AggregateByKey(this EventStoreV2 store, string key, Func<IEnumerable<double>, double> aggregator)
    {
        var keyId = new KeyMap().GetOrAdd(key);
        return AggregateByKeyId(store, keyId, aggregator);
    }
    
    /// <summary>
    /// Aggregates values for a specific key ID.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double AggregateByKeyId(this EventStoreV2 store, KeyId keyId, Func<IEnumerable<double>, double> aggregator)
    {
        var values = new List<double>();
        foreach (var e in QueryByKeyId(store, keyId))
        {
            values.Add(e.Value);
        }
        
        return values.Count > 0 ? aggregator(values) : 0;
    }
    
    /// <summary>
    /// Calculates the sum of values for a specific key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Sum(this EventStoreV2 store, string key)
    {
        return AggregateByKey(store, key, values => values.Sum());
    }
    
    /// <summary>
    /// Calculates the average of values for a specific key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Average(this EventStoreV2 store, string key)
    {
        return AggregateByKey(store, key, values => values.Any() ? values.Average() : 0);
    }
    
    /// <summary>
    /// Finds the minimum value for a specific key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Min(this EventStoreV2 store, string key)
    {
        return AggregateByKey(store, key, values => values.Any() ? values.Min() : 0);
    }
    
    /// <summary>
    /// Finds the maximum value for a specific key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Max(this EventStoreV2 store, string key)
    {
        return AggregateByKey(store, key, values => values.Any() ? values.Max() : 0);
    }
    
    /// <summary>
    /// Queries events within a time range.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Event> QueryByTimeRange(this EventStoreV2 store, DateTime from, DateTime to)
    {
        return QueryByTimeRangeTicks(store, from.Ticks, to.Ticks);
    }
      /// <summary>
    /// Queries events within a time range using ticks.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Event> QueryByTimeRangeTicks(this EventStoreV2 store, long fromTicks, long toTicks)
    {
        var results = new List<Event>();
        
        for (int i = 0; i < store.Partitions; i++)
        {
            var view = store.GetPartitionView(i);
            
            // Skip this partition if its time range doesn't overlap with the query
            if (view.Count > 0 && (view.ToTicks < fromTicks || view.FromTicks > toTicks))
            {
                continue;
            }
            
            foreach (var e in view)
            {
                if (e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks)
                {
                    results.Add(e);
                }
            }
        }
        
        return results;
    }
}

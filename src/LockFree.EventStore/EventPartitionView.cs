using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Specializations of PartitionView for the Event struct.
/// </summary>
public static class EventPartitionView
{
    /// <summary>
    /// Calculates the sum of all event values in a partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Sum(this PartitionView<Event> view)
    {
        double sum = 0;
        foreach (var e in view)
        {
            sum += e.Value;
        }
        return sum;
    }
    
    /// <summary>
    /// Calculates the average of all event values in a partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Average(this PartitionView<Event> view)
    {
        if (view.Count == 0) return 0;
        return view.Sum() / view.Count;
    }
    
    /// <summary>
    /// Finds the minimum value in a partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Min(this PartitionView<Event> view)
    {
        if (view.Count == 0) return 0;
        
        double min = double.MaxValue;
        foreach (var e in view)
        {
            if (e.Value < min) min = e.Value;
        }
        return min;
    }
    
    /// <summary>
    /// Finds the maximum value in a partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Max(this PartitionView<Event> view)
    {
        if (view.Count == 0) return 0;
        
        double max = double.MinValue;
        foreach (var e in view)
        {
            if (e.Value > max) max = e.Value;
        }
        return max;
    }
      /// <summary>
    /// Filters events by key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Event> WhereKey(this PartitionView<Event> view, KeyId key)
    {
        var results = new List<Event>();
        foreach (var e in view)
        {
            if (e.Key.Value == key.Value)
            {
                results.Add(e);
            }
        }
        return results;
    }
    
    /// <summary>
    /// Filters events by a time range.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Event> WhereTimeRange(this PartitionView<Event> view, long fromTicks, long toTicks)
    {
        var results = new List<Event>();
        foreach (var e in view)
        {
            if (e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks)
            {
                results.Add(e);
            }
        }
        return results;
    }
}

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Extension methods for performing batch operations on Event arrays with high performance.
/// </summary>
public static class EventBatchOperations
{
    /// <summary>
    /// Calculates the sum of values for a specific key in the event buffer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double SumByKey(ReadOnlySpan<Event> events, KeyId key)
    {
        double sum = 0;
        for (var i = 0; i < events.Length; i++)
        {
            if (events[i].Key.Value == key.Value)
            {
                sum += events[i].Value;
            }
        }
        return sum;
    }

    /// <summary>
    /// Calculates the average of values for a specific key in the event buffer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double AverageByKey(ReadOnlySpan<Event> events, KeyId key)
    {
        double sum = 0;
        var count = 0;

        for (var i = 0; i < events.Length; i++)
        {
            if (events[i].Key.Value == key.Value)
            {
                sum += events[i].Value;
                count++;
            }
        }

        return count > 0 ? sum / count : 0;
    }

    /// <summary>
    /// Filters events with timestamps in the inclusive range [fromTicks, toTicks].
    /// Writes up to <paramref name="output"/>.Length matching events into <paramref name="output"/> in the same order as they appear in <paramref name="events"/>.
    /// If more matches exist than the capacity of <paramref name="output"/>, the method truncates (clamps) to capacity and returns the number written.
    /// This method does not allocate and stops scanning early if the output buffer becomes full.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FilterByTimeRange(ReadOnlySpan<Event> events, long fromTicks, long toTicks, Span<Event> output)
    {
        var count = 0;
        for (var i = 0; i < events.Length; i++)
        {
            if (events[i].TimestampTicks >= fromTicks && events[i].TimestampTicks <= toTicks)
            {
                if (count < output.Length)
                {
                    output[count++] = events[i];
                }
                else
                {
                    break;
                }
            }
        }
        return count;
    }

    /// <summary>
    /// Gets the first and last timestamp from a span of events without sorting.
    /// Returns (0, 0) for an empty span. This reflects the timestamps of events[0] and events[^1].
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long firstTicks, long lastTicks) GetTimeRange(ReadOnlySpan<Event> events)
    {
        if (events.IsEmpty)
        {
            return (0, 0);
        }

        return (events[0].TimestampTicks, events[^1].TimestampTicks);
    }

    /// <summary>
    /// Performs a parallel aggregation of events by key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double[] AggregateByKeys(ReadOnlySpan<Event> events, ReadOnlySpan<KeyId> keys, Func<ReadOnlySpan<double>, double> aggregator)
    {
        if (keys.IsEmpty)
        {
            return [];
        }

        var results = new double[keys.Length];
        var values = new List<double>[keys.Length];

        for (var i = 0; i < keys.Length; i++)
        {
            values[i] = [];
        }

        // Collect values by key
        for (var i = 0; i < events.Length; i++)
        {
            for (var k = 0; k < keys.Length; k++)
            {
                if (events[i].Key.Value == keys[k].Value)
                {
                    values[k].Add(events[i].Value);
                    break;
                }
            }
        }

        // Apply aggregation function
        for (var i = 0; i < keys.Length; i++)
        {
            results[i] = values[i].Count > 0
                ? aggregator(CollectionsMarshal.AsSpan(values[i]))
                : 0;
        }

        return results;
    }
}

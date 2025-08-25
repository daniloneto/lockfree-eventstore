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
    /// <summary>
    /// Returns the sum of the Value fields for all events in <paramref name="events"/> whose key matches <paramref name="key"/>.
    /// </summary>
    /// <param name="events">Span of events to scan. The match is performed by comparing each event's <c>Key.Value</c> to <paramref name="key"/>.</param>
    /// <param name="key">Key to match against each event's <c>Key.Value</c>.</param>
    /// <returns>The total sum of matching events' Value; 0 if no events match.</returns>
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
    /// <summary>
    /// Computes the arithmetic mean of the Value field for all events whose Key matches the provided key.
    /// </summary>
    /// <param name="events">Span of events to scan (order not important for the result).</param>
    /// <param name="key">Key to match against each event's Key.Value.</param>
    /// <returns>The average of matching event values, or 0 if no events match.</returns>
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
    /// <summary>
    /// Copies events whose <c>TimestampTicks</c> fall within the inclusive range [fromTicks, toTicks] into <paramref name="output"/> and returns how many were written.
    /// </summary>
    /// <param name="events">Input span of events to scan.</param>
    /// <param name="fromTicks">Inclusive lower bound of the timestamp range.</param>
    /// <param name="toTicks">Inclusive upper bound of the timestamp range.</param>
    /// <param name="output">Destination span receiving matching events; writing stops when this span is full.</param>
    /// <returns>The number of events written into <paramref name="output"/> (may be less than the number of matches if <paramref name="output"/> is not large enough).</returns>
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
    /// <summary>
    /// Returns the first and last event timestamp ticks from the provided span.
    /// </summary>
    /// <param name="events">Events in their current (stored) order. No sorting is performed.</param>
    /// <returns>
    /// A tuple (firstTicks, lastTicks). If <paramref name="events"/> is empty both values are 0; otherwise the timestamps of the first and last elements.
    /// </returns>
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
    /// <summary>
    /// Aggregates event values for each key using the provided aggregator function.
    /// </summary>
    /// <remarks>
    /// For each key in <paramref name="keys"/>, collects the Value of events whose Key.Value equals the key's Value,
    /// then applies <paramref name="aggregator"/> to the collected values to produce the result for that key.
    /// The input order is preserved when collecting values. If a key has no matching events, the result for that key is 0
    /// and the aggregator is not invoked for that key.
    /// </remarks>
    /// <param name="events">Span of events to scan for values.</param>
    /// <param name="keys">Span of keys to aggregate values for; results correspond to this order.</param>
    /// <param name="aggregator">Function that computes a single double from a ReadOnlySpan&lt;double&gt; of values for a key.</param>
    /// <returns>
    /// An array of doubles with the same length and order as <paramref name="keys"/>, where each element is the aggregation
    /// result for the corresponding key (or 0 if no events matched that key).
    /// </returns>
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

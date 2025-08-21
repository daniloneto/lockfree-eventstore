using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Delegate for processing events during zero-allocation enumeration.
/// </summary>
/// <typeparam name="TEvent">The event type</typeparam>
/// <typeparam name="TState">The accumulator state type</typeparam>
/// <param name="state">The accumulator state (passed by reference for performance)</param>
/// <param name="evt">The current event being processed</param>
/// <param name="timestamp">The event timestamp (if available)</param>
/// <returns>True to continue processing, false to stop early</returns>
public delegate bool EventProcessor<TEvent, TState>(ref TState state, TEvent evt, DateTime? timestamp = null);

/// <summary>
/// Delegate for filtering events during zero-allocation enumeration.
/// </summary>
/// <typeparam name="TEvent">The event type</typeparam>
/// <param name="evt">The event to test</param>
/// <param name="timestamp">The event timestamp (if available)</param>
/// <returns>True if the event should be processed, false to skip</returns>
public delegate bool EventFilter<TEvent>(TEvent evt, DateTime? timestamp = null);

/// <summary>
/// Extensions for zero-allocation event processing operations.
/// </summary>
public static class ZeroAllocationExtensions
{
    /// <summary>
    /// Processes all events in the store using a zero-allocation callback approach.
    /// No intermediate collections are created.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TState ProcessEvents<TEvent, TState>(
        this EventStore<TEvent> store,
        TState initialState,
        EventProcessor<TEvent, TState> processor,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        var state = initialState;
        var views = store.SnapshotViews(from, to);
          foreach (var view in views)
        {
            if (!ProcessPartitionView(view, ref state, processor, filter, store.TimestampSelector))
                break; // Early termination requested
        }
        
        return state;
    }

    /// <summary>
    /// Counts events matching the filter without allocating intermediate collections.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long CountEventsZeroAlloc<TEvent>(
        this EventStore<TEvent> store,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        return store.ProcessEvents<TEvent, long>(
            0L,
            (ref long count, TEvent evt, DateTime? timestamp) =>
            {
                count++;
                return true; // Continue processing
            },
            filter,
            from,
            to);
    }

    /// <summary>
    /// Finds the first event matching the criteria without allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (bool Found, TEvent Event) FindFirstZeroAlloc<TEvent>(
        this EventStore<TEvent> store,
        EventFilter<TEvent> filter,
        DateTime? from = null,
        DateTime? to = null)
    {
        var result = store.ProcessEvents<TEvent, (bool Found, TEvent Event)>(
            (false, default(TEvent)!),
            (ref (bool Found, TEvent Event) state, TEvent evt, DateTime? timestamp) =>
            {
                state.Found = true;
                state.Event = evt;
                return false; // Stop on first match
            },
            filter,
            from,
            to);
        
        return result;
    }

    /// <summary>
    /// Computes a custom aggregation without allocating intermediate collections.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TAcc AggregateZeroAlloc<TEvent, TAcc>(
        this EventStore<TEvent> store,
        TAcc seed,
        Func<TAcc, TEvent, TAcc> accumulator,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        return store.ProcessEvents<TEvent, TAcc>(
            seed,
            (ref TAcc state, TEvent evt, DateTime? timestamp) =>
            {
                state = accumulator(state, evt);
                return true; // Continue processing
            },
            filter,
            from,
            to);
    }    /// <summary>
    /// Processes events in chunks without allocation using pooled buffers.
    /// More efficient than ProcessEvents for very large datasets.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TState ProcessEventsChunked<TEvent, TState>(
        this EventStore<TEvent> store,
        TState initialState,
        Func<TState, ReadOnlySpan<TEvent>, TState> chunkProcessor,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null,
        int chunkSize = Buffers.DefaultChunkSize)
    {
        var state = initialState;
        
        if (filter != null)
        {
            store.SnapshotFilteredZeroAlloc(evt =>
            {
                var timestamp = store.TimestampSelector?.GetTimestamp(evt);
                return filter(evt, timestamp);
            }, chunk =>
            {
                state = chunkProcessor(state, chunk);
            }, chunkSize);
        }
        else
        {
            store.SnapshotZeroAlloc(chunk =>
            {
                state = chunkProcessor(state, chunk);
            }, chunkSize);
        }
        
        return state;
    }    /// <summary>
    /// Sums numeric values from events without allocation using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double SumZeroAlloc<TEvent>(
        this EventStore<TEvent> store,
        Func<TEvent, double> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        return store.ProcessEventsChunked<TEvent, double>(
            0.0,
            (sum, chunk) =>
            {
                foreach (var evt in chunk)
                {
                    sum += selector(evt);
                }
                return sum;
            },
            filter,
            from,
            to);
    }

    /// <summary>
    /// Finds minimum value from events without allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TResult? MinZeroAlloc<TEvent, TResult>(
        this EventStore<TEvent> store,
        Func<TEvent, TResult> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        var result = store.ProcessEventsChunked<TEvent, (bool HasValue, TResult Min)>(
            (false, default(TResult)),
            (state, chunk) =>
            {
                foreach (var evt in chunk)
                {
                    var value = selector(evt);
                    if (!state.HasValue || value.CompareTo(state.Min) < 0)
                    {
                        state = (true, value);
                    }
                }
                return state;
            },
            filter,
            from,
            to);
        
        return result.HasValue ? result.Min : null;
    }

    /// <summary>
    /// Finds maximum value from events without allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TResult? MaxZeroAlloc<TEvent, TResult>(
        this EventStore<TEvent> store,
        Func<TEvent, TResult> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
        where TResult : struct, IComparable<TResult>
    {
        var result = store.ProcessEventsChunked<TEvent, (bool HasValue, TResult Max)>(
            (false, default(TResult)),
            (state, chunk) =>
            {
                foreach (var evt in chunk)
                {
                    var value = selector(evt);
                    if (!state.HasValue || value.CompareTo(state.Max) > 0)
                    {
                        state = (true, value);
                    }
                }
                return state;
            },
            filter,
            from,
            to);
        
        return result.HasValue ? result.Max : null;
    }    /// <summary>
    /// Calculates average of numeric values from events without allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double AverageZeroAlloc<TEvent>(
        this EventStore<TEvent> store,
        Func<TEvent, double> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        var result = store.ProcessEventsChunked<TEvent, (double Sum, long Count)>(
            (0.0, 0L),
            (state, chunk) =>
            {
                foreach (var evt in chunk)
                {
                    state.Sum += selector(evt);
                    state.Count++;
                }
                return state;
            },
            filter,
            from,
            to);
        
        return result.Count > 0 ? result.Sum / result.Count : 0.0;
    }

    /// <summary>
    /// Groups events by key without allocation, processing results in chunks.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void GroupByZeroAlloc<TEvent, TKey>(
        this EventStore<TEvent> store,
        Func<TEvent, TKey> keySelector,
        Action<ReadOnlySpan<KeyValuePair<TKey, List<TEvent>>>> processor,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null,
        int chunkSize = Buffers.DefaultChunkSize)
        where TKey : notnull
    {
        var groups = new Dictionary<TKey, List<TEvent>>();

        // Process events in chunks and accumulate into groups
        store.ProcessEventsChunked<TEvent, Dictionary<TKey, List<TEvent>>>(
            groups,
            (currentGroups, chunk) => AccumulateGroups(currentGroups, chunk, keySelector),
            filter,
            from,
            to,
            chunkSize);

        // Emit grouped results in chunks
        EmitGroupsChunks(groups, processor, chunkSize);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Dictionary<TKey, List<TEvent>> AccumulateGroups<TEvent, TKey>(
        Dictionary<TKey, List<TEvent>> currentGroups,
        ReadOnlySpan<TEvent> chunk,
        Func<TEvent, TKey> keySelector)
        where TKey : notnull
    {
        for (int i = 0; i < chunk.Length; i++)
        {
            var evt = chunk[i];
            var key = keySelector(evt);
            if (!currentGroups.TryGetValue(key, out var list))
            {
                list = new List<TEvent>();
                currentGroups[key] = list;
            }
            list.Add(evt);
        }
        return currentGroups;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EmitGroupsChunks<TEvent, TKey>(
        Dictionary<TKey, List<TEvent>> groups,
        Action<ReadOnlySpan<KeyValuePair<TKey, List<TEvent>>>> processor,
        int chunkSize)
        where TKey : notnull
    {
        if (groups.Count == 0) return;

        Buffers.WithRentedBuffer<KeyValuePair<TKey, List<TEvent>>>(chunkSize, buffer =>
        {
            var count = 0;
            foreach (var kvp in groups)
            {
                buffer[count++] = kvp;
                if (count >= buffer.Length)
                {
                    processor(buffer.AsSpan(0, count));
                    count = 0;
                }
            }
            if (count > 0)
            {
                processor(buffer.AsSpan(0, count));
            }
        }, ArrayPool<KeyValuePair<TKey, List<TEvent>>>.Shared);
    }

    /// <summary>
    /// Internal helper to process a single partition view.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ProcessPartitionView<TEvent, TState>(
        PartitionView<TEvent> view,
        ref TState state,
        EventProcessor<TEvent, TState> processor,
        EventFilter<TEvent>? filter,
        IEventTimestampSelector<TEvent>? timestampSelector)
    {
        if (view.IsEmpty) return true;

        foreach (var evt in view)
        {
            DateTime? timestamp = timestampSelector?.GetTimestamp(evt);
            
            // Apply filter if provided
            if (filter != null && !filter(evt, timestamp))
                continue;
            
            // Process the event
            if (!processor(ref state, evt, timestamp))
                return false; // Early termination requested
        }
          return true; // Continue processing
    }
}

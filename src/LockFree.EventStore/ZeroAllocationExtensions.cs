using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Delegate for processing events during zero-allocation enumeration.
/// </summary>
public delegate bool EventProcessor<TEvent, TState>(ref TState state, TEvent evt, DateTime? timestamp = null);

/// <summary>
/// Delegate for filtering events during zero-allocation enumeration.
/// </summary>
public delegate bool EventFilter<TEvent>(TEvent evt, DateTime? timestamp = null);

/// <summary>
/// Extensions for zero-allocation event processing operations.
/// </summary>
public static class ZeroAllocationExtensions
{
    // Helper to centralize filter application and timestamp acquisition
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool PassesFilter<TEvent>(
        EventFilter<TEvent>? filter,
        IEventTimestampSelector<TEvent>? tsSelector,
        TEvent evt)
    {
        if (filter is null)
        {
            return true;
        }
        var ts = tsSelector?.GetTimestamp(evt);
        return filter(evt, ts);
    }

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
        // Use unfiltered views when no time bounds are provided to avoid requiring a TimestampSelector
        var hasTime = from.HasValue || to.HasValue;
        var views = hasTime ? store.SnapshotViews(from, to) : store.SnapshotViews();
        foreach (var view in views)
        {
            if (!ProcessPartitionView(view, ref state, processor, filter, store.TimestampSelector))
            {
                break; // Early termination requested
            }
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
        bool Processor(ref long count, TEvent evt, DateTime? timestamp)
        {
            if (filter == null || filter(evt, timestamp))
            {
                count++;
            }
            return true;
        }

        return store.ProcessEvents(0L, Processor, null, from, to);
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
        bool Processor(ref (bool Found, TEvent Event) state, TEvent evt, DateTime? timestamp)
        {
            if (!filter(evt, timestamp))
            {
                return true;
            }
            state.Found = true;
            state.Event = evt;
            return false; // Stop on first match
        }

        var result = store.ProcessEvents<TEvent, (bool Found, TEvent Event)>((false, default(TEvent)!), Processor, null, from, to);
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
        bool Processor(ref TAcc state, TEvent evt, DateTime? timestamp)
        {
            if (filter == null || filter(evt, timestamp))
            {
                state = accumulator(state, evt);
            }
            return true;
        }

        return store.ProcessEvents(seed, Processor, null, from, to);
    }

    /// <summary>
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
        var hasTime = from.HasValue || to.HasValue;

        if (filter is null)
        {
            if (hasTime)
            {
                store.SnapshotTimeFilteredZeroAlloc(from, to, chunk =>
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
        }
        else
        {
            if (!hasTime)
            {
                // Let the ring buffer perform the filtering, then process chunks.
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
                // Time window present: stream time-filtered chunks and apply filter using a pooled scratch buffer.
                var pool = ArrayPool<TEvent>.Shared;
                var scratch = pool.Rent(chunkSize);
                try
                {
                    store.SnapshotTimeFilteredZeroAlloc(from, to, chunk =>
                    {
                        var n = 0;
                        for (var i = 0; i < chunk.Length; i++)
                        {
                            var evt = chunk[i];
                            if (PassesFilter(filter, store.TimestampSelector, evt))
                            {
                                if (n >= scratch.Length)
                                {
                                    // Expand scratch if necessary (rare); rent a larger buffer.
                                    var old = scratch;
                                    scratch = pool.Rent(Math.Max(old.Length * 2, n + 1));
                                    old.AsSpan(0, n).CopyTo(scratch);
                                    pool.Return(old, clearArray: false);
                                }
                                scratch[n++] = evt;
                            }
                        }
                        if (n > 0)
                        {
                            state = chunkProcessor(state, scratch.AsSpan(0, n));
                        }
                    }, chunkSize);
                }
                finally
                {
                    pool.Return(scratch, clearArray: false);
                }
            }
        }

        return state;
    }

    /// <summary>
    /// Sums numeric values (generic math) without allocation using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TValue SumZeroAlloc<TEvent, TValue>(
        this EventStore<TEvent> store,
        Func<TEvent, TValue> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        return store.ProcessEventsChunked(
            TValue.Zero,
            (sum, chunk) =>
            {
                for (var i = 0; i < chunk.Length; i++)
                {
                    var evt = chunk[i];
                    if (PassesFilter(filter, store.TimestampSelector, evt))
                    {
                        sum += selector(evt);
                    }
                }
                return sum;
            },
            filter,
            from,
            to);
    }

    /// <summary>
    /// Sums numeric values from events without allocation using chunked processing (double selector fast-path).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double SumZeroAlloc<TEvent>(
        this EventStore<TEvent> store,
        Func<TEvent, double> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        return store.ProcessEventsChunked(
            0.0,
            (sum, chunk) =>
            {
                for (var i = 0; i < chunk.Length; i++)
                {
                    var evt = chunk[i];
                    if (PassesFilter(filter, store.TimestampSelector, evt))
                    {
                        sum += selector(evt);
                    }
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
        var (hasValue, min) = store.ProcessEventsChunked<TEvent, (bool HasValue, TResult Min)>(
            (false, default),
            (state, chunk) =>
            {
                for (var i = 0; i < chunk.Length; i++)
                {
                    var evt = chunk[i];
                    if (!PassesFilter(filter, store.TimestampSelector, evt))
                    {
                        continue;
                    }
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

        return hasValue ? min : null;
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
        var (hasValue, max) = store.ProcessEventsChunked<TEvent, (bool HasValue, TResult Max)>(
            (false, default),
            (state, chunk) =>
            {
                for (var i = 0; i < chunk.Length; i++)
                {
                    var evt = chunk[i];
                    if (!PassesFilter(filter, store.TimestampSelector, evt))
                    {
                        continue;
                    }
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

        return hasValue ? max : null;
    }

    /// <summary>
    /// Calculates average of numeric values from events without allocation (generic math).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double AverageZeroAlloc<TEvent, TValue>(
        this EventStore<TEvent> store,
        Func<TEvent, TValue> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
        where TValue : struct, INumber<TValue>
    {
        var (sum, count) = store.ProcessEventsChunked<TEvent, (TValue Sum, long Count)>(
            (TValue.Zero, 0L),
            (state, chunk) =>
            {
                for (var i = 0; i < chunk.Length; i++)
                {
                    var evt = chunk[i];
                    if (!PassesFilter(filter, store.TimestampSelector, evt))
                    {
                        continue;
                    }
                    state.Sum += selector(evt);
                    state.Count++;
                }
                return state;
            },
            filter,
            from,
            to);

        return count > 0 ? double.CreateChecked(sum) / count : 0.0;
    }

    /// <summary>
    /// Calculates average of numeric values from events without allocation (double selector fast-path).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double AverageZeroAlloc<TEvent>(
        this EventStore<TEvent> store,
        Func<TEvent, double> selector,
        EventFilter<TEvent>? filter = null,
        DateTime? from = null,
        DateTime? to = null)
    {
        var (sum, count) = store.ProcessEventsChunked<TEvent, (double Sum, long Count)>(
            (0.0, 0L),
            (state, chunk) =>
            {
                for (var i = 0; i < chunk.Length; i++)
                {
                    var evt = chunk[i];
                    if (!PassesFilter(filter, store.TimestampSelector, evt))
                    {
                        continue;
                    }
                    state.Sum += selector(evt);
                    state.Count++;
                }
                return state;
            },
            filter,
            from,
            to);

        return count > 0 ? sum / count : 0.0;
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
        groups = store.ProcessEventsChunked(
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
        for (var i = 0; i < chunk.Length; i++)
        {
            var evt = chunk[i];
            var key = keySelector(evt);
            if (currentGroups.TryGetValue(key, out var existing))
            {
                existing.Add(evt);
            }
            else
            {
                currentGroups[key] = [evt];
            }
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
        if (groups.Count == 0)
        {
            return;
        }

        Buffers.WithRentedBuffer(chunkSize, buffer =>
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
        if (view.IsEmpty)
        {
            return true;
        }

        foreach (var evt in view)
        {
            var timestamp = timestampSelector?.GetTimestamp(evt);

            // Apply filter if provided
            if (filter != null && !filter(evt, timestamp))
            {
                continue;
            }

            // Process the event
            if (!processor(ref state, evt, timestamp))
            {
                return false; // Early termination requested
            }
        }
        return true; // Continue processing
    }
}

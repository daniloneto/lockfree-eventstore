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
    /// <summary>
    /// Determines whether an event passes the optional filter, acquiring an event timestamp if a timestamp selector is provided.
    /// </summary>
    /// <param name="filter">An optional predicate to test the event. If null, the event is considered to pass.</param>
    /// <param name="tsSelector">An optional timestamp selector; if provided its <c>GetTimestamp</c> result is passed to the filter.</param>
    /// <param name="evt">The event to evaluate.</param>
    /// <returns><c>true</c> when no filter is supplied or when the filter returns <c>true</c> for the event (with the selected timestamp if available); otherwise <c>false</c>.</returns>
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
    /// Determines whether a time range is specified by at least one boundary.
    /// </summary>
    /// <param name="from">Optional start (inclusive) of the time window; null if unbounded.</param>
    /// <param name="to">Optional end (inclusive) of the time window; null if unbounded.</param>
    /// <returns>True when either <paramref name="from"/> or <paramref name="to"/> has a value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasTimeRange(DateTime? from, DateTime? to)
    {
        return from.HasValue || to.HasValue;
    }

    /// <summary>
    /// Processes events in chunked batches using a fast, no-allocation path and applies a chunk-level processor to update an accumulated state.
    /// </summary>
    /// <remarks>
    /// Chooses between a time-filtered snapshot and an unfiltered snapshot based on <paramref name="hasTime"/> and invokes <paramref name="chunkProcessor"/>
    /// for each chunk. This method does not allocate per-event; it delegates to the store's zero-allocation snapshot readers.
    /// </remarks>
    /// <param name="state">The initial accumulator state; updated and returned after processing all chunks.</param>
    /// <param name="chunkProcessor">A callback that receives the current state and a read-only span of events for the chunk, and returns the updated state.</param>
    /// <param name="from">Optional lower time bound used when <paramref name="hasTime"/> is true.</param>
    /// <param name="to">Optional upper time bound used when <paramref name="hasTime"/> is true.</param>
    /// <param name="hasTime">When true, the <paramref name="from"/>/ <paramref name="to"/> range is applied and a time-filtered snapshot is used.</param>
    /// <param name="chunkSize">Requested chunk size passed to the snapshot reader to control batch sizes.</param>
    /// <returns>The accumulator state after processing all chunks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TState ProcessNoFilterChunked<TEvent, TState>(
        EventStore<TEvent> store,
        TState state,
        Func<TState, ReadOnlySpan<TEvent>, TState> chunkProcessor,
        DateTime? from,
        DateTime? to,
        bool hasTime,
        int chunkSize)
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

        return state;
    }

    /// <summary>
    /// Process events using the store's filtered snapshot path (no explicit time bounds) and apply a chunk processor to each accepted chunk.
    /// </summary>
    /// <param name="state">The initial processing state; returned state reflects updates from the chunk processor.</param>
    /// <param name="chunkProcessor">Callback that accepts the current state and a span of events (a chunk) and returns the updated state.</param>
    /// <param name="filter">Predicate that determines which events to include; if the store provides a timestamp selector it will be passed as the optional timestamp to this filter.</param>
    /// <param name="chunkSize">Preferred chunk size used when invoking the store snapshot reader.</param>
    /// <returns>The final state after processing all filtered chunks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TState ProcessWithFilterNoTime<TEvent, TState>(
        EventStore<TEvent> store,
        TState state,
        Func<TState, ReadOnlySpan<TEvent>, TState> chunkProcessor,
        EventFilter<TEvent> filter,
        int chunkSize)
    {
        store.SnapshotFilteredZeroAlloc(evt =>
        {
            var timestamp = store.TimestampSelector?.GetTimestamp(evt);
            return filter(evt, timestamp);
        }, chunk =>
        {
            state = chunkProcessor(state, chunk);
        }, chunkSize);

        return state;
    }

    /// <summary>
    /// Processes time-windowed event chunks from the store, applies an event filter, and invokes a chunk processor on the filtered events using a pooled scratch buffer.
    /// </summary>
    /// <remarks>
    /// Reads chunks that fall within the optional <paramref name="from"/>/ <paramref name="to"/> range, copies only the events that pass <paramref name="filter"/> into a pooled array, and calls <paramref name="chunkProcessor"/> for each non-empty filtered span. The pooled buffer is returned to the array pool when processing completes.
    /// </remarks>
    /// <param name="state">Aggregation state that is passed to and returned from <paramref name="chunkProcessor"/>.</param>
    /// <param name="chunkProcessor">Callback that consumes a contiguous span of filtered events and returns the updated state.</param>
    /// <param name="filter">Predicate applied to each event (the store's timestamp selector is used for any time-based filter logic).</param>
    /// <param name="from">Inclusive lower time bound for events; pass null for no lower bound.</param>
    /// <param name="to">Inclusive upper time bound for events; pass null for no upper bound.</param>
    /// <param name="chunkSize">Initial rented buffer size and preferred chunk size used when reading from the store.</param>
    /// <returns>The final state after all matching events have been processed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TState ProcessWithFilterAndTime<TEvent, TState>(
        EventStore<TEvent> store,
        TState state,
        Func<TState, ReadOnlySpan<TEvent>, TState> chunkProcessor,
        EventFilter<TEvent> filter,
        DateTime? from,
        DateTime? to,
        int chunkSize)
    {
        var pool = ArrayPool<TEvent>.Shared;
        var scratch = pool.Rent(chunkSize);
        try
        {
            store.SnapshotTimeFilteredZeroAlloc(from, to, chunk =>
            {
                var n = FilterChunkIntoScratch(chunk, filter, store.TimestampSelector, ref scratch, pool);
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

        return state;
    }

    /// <summary>
    /// Copies events from <paramref name="chunk"/> that satisfy <paramref name="filter"/> into <paramref name="scratch"/>.
    /// </summary>
    /// <remarks>
    /// The method writes matching events into <paramref name="scratch"/> and returns the number of written items.
    /// If <paramref name="scratch"/> is too small it is replaced by a larger array rented from <c>pool</c> and the previous array is returned to the pool.
    /// The <paramref name="scratch"/> reference is updated to point to the (possibly re-rented) buffer containing the copied events.
    /// </remarks>
    /// <param name="chunk">Source events to evaluate.</param>
    /// <param name="filter">Predicate that determines whether an event should be copied.</param>
    /// <param name="tsSelector">Optional timestamp selector used by the filter when timestamps are required.</param>
    /// <param name="scratch">Buffer to receive filtered events; may be replaced with a larger pooled array if resizing is needed.</param>
    /// <returns>The number of events copied into <paramref name="scratch"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FilterChunkIntoScratch<TEvent>(
        ReadOnlySpan<TEvent> chunk,
        EventFilter<TEvent> filter,
        IEventTimestampSelector<TEvent>? tsSelector,
        ref TEvent[] scratch,
        ArrayPool<TEvent> pool)
    {
        var n = 0;
        for (var i = 0; i < chunk.Length; i++)
        {
            var evt = chunk[i];
            if (PassesFilter(filter, tsSelector, evt))
            {
                if (n >= scratch.Length)
                {
                    var old = scratch;
                    scratch = pool.Rent(Math.Max(old.Length * 2, n + 1));
                    old.AsSpan(0, n).CopyTo(scratch);
                    pool.Return(old, clearArray: false);
                }
                scratch[n++] = evt;
            }
        }
        return n;
    }

    /// <summary>
    /// Processes all events in the store using a zero-allocation callback approach.
    /// No intermediate collections are created.
    /// <summary>
    /// Processes events from the store, updating and returning an accumulated state using a zero-allocation processor callback.
    /// </summary>
    /// <remarks>
    /// Iterates snapshot views (optionally constrained by <paramref name="from"/> and <paramref name="to"/>)
    /// and invokes <paramref name="processor"/> for each event that passes <paramref name="filter"/> (if provided).
    /// If no time bounds are supplied, unfiltered snapshot views are used to avoid requiring a timestamp selector on the store.
    /// The provided <paramref name="processor"/> may stop processing early by returning <c>false</c>.
    /// </remarks>
    /// <param name="initialState">The initial accumulator state; returned (possibly mutated) after processing completes.</param>
    /// <param name="processor">Callback that receives a reference to the state, the event and an optional timestamp. Return <c>false</c> to stop processing early.</param>
    /// <param name="filter">Optional predicate to select events; if <c>null</c> all events are considered.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps; when set, timestamp-aware snapshot views are used.</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps; when set, timestamp-aware snapshot views are used.</param>
    /// <returns>The final accumulated state after processing (or earlier if the processor requested termination).</returns>
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
    /// <summary>
    /// Counts events in the store that match an optional filter and optional time range, without allocating.
    /// </summary>
    /// <param name="filter">Optional predicate to select events; invoked with the event and its timestamp. If null all events are counted.</param>
    /// <param name="from">Inclusive lower bound of the timestamp window; null means no lower bound.</param>
    /// <param name="to">Inclusive upper bound of the timestamp window; null means no upper bound.</param>
    /// <returns>The number of events that match the filter and fall within the specified time range.</returns>
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
    /// <summary>
    /// Finds the first event in the store that satisfies the provided filter within an optional time window.
    /// </summary>
    /// <param name="filter">Predicate invoked for each event (and its optional timestamp); the first event for which this returns true is returned.</param>
    /// <param name="from">Inclusive lower bound of the timestamp window to search, or null for no lower bound.</param>
    /// <param name="to">Inclusive upper bound of the timestamp window to search, or null for no upper bound.</param>
    /// <returns>
    /// A tuple where <c>Found</c> is true and <c>Event</c> is the matching event when a match exists; otherwise <c>Found</c> is false and <c>Event</c> is the default value for <typeparamref name="TEvent"/>.
    /// </returns>
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
    /// <summary>
    /// Aggregates events from the store into an accumulator using a zero-allocation callback path.
    /// </summary>
    /// <param name="seed">Initial accumulator value.</param>
    /// <param name="accumulator">Function that updates the accumulator for each accepted event: (current, event) => next.</param>
    /// <param name="filter">Optional predicate to include an event; receives the event and its optional timestamp.</param>
    /// <param name="from">Optional lower bound of the time window to process (inclusive when provided).</param>
    /// <param name="to">Optional upper bound of the time window to process (inclusive when provided).</param>
    /// <returns>The final accumulator value after processing all matching events.</returns>
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
    /// <summary>
    /// Processes events from the store in fixed-size chunks, updating and returning an aggregated state.
    /// </summary>
    /// <remarks>
    /// Chooses an optimized path based on whether an <paramref name="filter"/> and/or a time window (<paramref name="from"/>/<paramref name="to"/>) are provided:
    /// - No filter: processes chunks directly (optionally time-filtered).
    /// - Filter without time bounds: applies the filter per event while chunking.
    /// - Filter with time bounds: collects filtered events into a pooled scratch buffer before invoking the chunk processor.
    /// The implementation is zero-allocation oriented and may short-circuit internal iteration when appropriate.
    /// </remarks>
    /// <param name="initialState">The starting state to be updated by the chunk processor.</param>
    /// <param name="chunkProcessor">A callback that receives the current state and a span of events (one chunk) and returns the updated state.</param>
    /// <param name="filter">Optional predicate to select events; if null all events are accepted.</param>
    /// <param name="from">Optional lower time bound for events; if null there is no lower bound.</param>
    /// <param name="to">Optional upper time bound for events; if null there is no upper bound.</param>
    /// <param name="chunkSize">The maximum number of events passed to <paramref name="chunkProcessor"/> per invocation.</param>
    /// <returns>The final state after processing all matching events.</returns>
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
        var hasTime = HasTimeRange(from, to);

        return filter is null
            ? ProcessNoFilterChunked(store, state, chunkProcessor, from, to, hasTime, chunkSize)
            : (!hasTime
                ? ProcessWithFilterNoTime(store, state, chunkProcessor, filter, chunkSize)
                : ProcessWithFilterAndTime(store, state, chunkProcessor, filter, from, to, chunkSize));
    }

    /// <summary>
    /// Sums numeric values (generic math) without allocation using chunked processing.
    /// <summary>
    /// Computes the sum of values produced by <paramref name="selector"/> for events that pass an optional filter and lie within an optional time range, using a zero-allocation chunked implementation.
    /// </summary>
    /// <param name="selector">Maps an event to the numeric value to add to the sum.</param>
    /// <param name="filter">Optional predicate to include events; when null all events are considered.</param>
    /// <param name="from">Optional inclusive start timestamp to restrict considered events; null means no lower bound.</param>
    /// <param name="to">Optional inclusive end timestamp to restrict considered events; null means no upper bound.</param>
    /// <returns>The accumulated sum of selected values; returns the numeric zero if no events match.</returns>
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
    /// <summary>
    /// Computes the sum of a double-valued projection over events that match an optional filter and time window, performing the aggregation without allocating intermediate collections.
    /// </summary>
    /// <param name="selector">Projection that maps an event to the double value to include in the sum.</param>
    /// <param name="filter">Optional predicate to select events. If provided and time bounds are given, the store's timestamp selector is used to evaluate the filter within the specified window.</param>
    /// <param name="from">Optional inclusive start of the time window. If null, no lower time bound is applied.</param>
    /// <param name="to">Optional inclusive end of the time window. If null, no upper time bound is applied.</param>
    /// <returns>The sum of selected values as a double. Returns 0.0 if no events match.</returns>
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
    /// <summary>
    /// Computes the minimum selected value across events without allocating intermediate collections.
    /// </summary>
    /// <param name="selector">Maps an event to the value to compare.</param>
    /// <param name="filter">Optional predicate to include only matching events; if null all events are considered.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps; if null no lower bound is applied.</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps; if null no upper bound is applied.</param>
    /// <returns>The minimum selected value among events that pass the filter and time bounds, or <c>null</c> if no events match.</returns>
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
    /// <summary>
    /// Computes the maximum projected value across events without allocating, returning null if no events match.
    /// </summary>
    /// <typeparam name="TResult">A value type whose instances can be compared (implements <c>IComparable&lt;TResult&gt;</c>).</typeparam>
    /// <param name="selector">Projects an event to the value to compare.</param>
    /// <param name="filter">Optional predicate to select events; if null all events are considered.</param>
    /// <param name="from">Optional inclusive lower bound on event timestamp.</param>
    /// <param name="to">Optional inclusive upper bound on event timestamp.</param>
    /// <returns>The maximum selected value among events that pass the optional filter and time window, or <c>null</c> if none match.</returns>
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
    /// <summary>
    /// Computes the average of values projected from events using a selector, without allocating intermediate collections.
    /// </summary>
    /// <param name="selector">Projects a numeric value from each event.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive lower bound for event timestamps.</param>
    /// <param name="to">Optional inclusive upper bound for event timestamps.</param>
    /// <returns>The average as a <see cref="double"/> of all selected values that pass the filter and time bounds; returns 0.0 if no events match.</returns>
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
    /// <summary>
    /// Computes the average of a double-valued projection from events that optionally match a filter and time range, using a zero-allocation chunked processing path.
    /// </summary>
    /// <param name="selector">Projection that maps an event to a double value to include in the average.</param>
    /// <param name="filter">Optional predicate to select which events to include. If null all events are considered.</param>
    /// <param name="from">Optional inclusive lower bound on event timestamps. If null no lower bound is applied.</param>
    /// <param name="to">Optional inclusive upper bound on event timestamps. If null no upper bound is applied.</param>
    /// <returns>The average of selected values, or 0.0 if no events matched.</returns>
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
    /// <summary>
    /// Groups events by a key and invokes a chunked processor for the resulting groups without producing intermediate enumerable allocations.
    /// </summary>
    /// <remarks>
    /// Accumulates events from the store into a dictionary of lists keyed by <typeparamref name="TKey"/>, then streams those key/list pairs to <paramref name="processor"/> in contiguous chunks of up to <paramref name="chunkSize"/> elements.
    /// Respects the optional <paramref name="filter"/> and time window defined by <paramref name="from"/> and <paramref name="to"/> while reading events.
    /// </remarks>
    /// <typeparam name="TEvent">Event type stored in the <see cref="EventStore{TEvent}"/>.</typeparam>
    /// <typeparam name="TKey">Group key type.</typeparam>
    /// <param name="keySelector">Maps an event to its grouping key.</param>
    /// <param name="processor">Called for each chunk of grouped results as a read-only span of key/list pairs.</param>
    /// <param name="filter">Optional predicate to include only matching events.</param>
    /// <param name="from">Optional inclusive start of the time window to consider.</param>
    /// <param name="to">Optional inclusive end of the time window to consider.</param>
    /// <param name="chunkSize">Maximum number of groups passed to <paramref name="processor"/> per invocation.</param>
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

    /// <summary>
    /// Adds each event from <paramref name="chunk"/> into <paramref name="currentGroups"/>, grouping by the key produced by <paramref name="keySelector"/>.
    /// </summary>
    /// <param name="currentGroups">A dictionary mapping keys to lists of events to be populated. New lists are created for keys not yet present.</param>
    /// <param name="chunk">A span of events to accumulate into the groups.</param>
    /// <param name="keySelector">Function that produces the grouping key for an event.</param>
    /// <returns>The same <paramref name="currentGroups"/> instance after all events from <paramref name="chunk"/> have been appended to their corresponding group lists.</returns>
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

    /// <summary>
    /// Emits the contents of <paramref name="groups"/> to <paramref name="processor"/> in contiguous chunks of up to <paramref name="chunkSize"/>,
    /// using a pooled buffer to avoid allocations. Returns immediately if <paramref name="groups"/> is empty.
    /// </summary>
    /// <param name="groups">Dictionary mapping keys to their grouped event lists.</param>
    /// <param name="processor">Action invoked for each emitted chunk; receives a ReadOnlySpan of the key/value pairs for that chunk.</param>
    /// <param name="chunkSize">Maximum number of pairs to include in each chunk passed to <paramref name="processor"/>.</param>
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
    /// <summary>
    /// Processes the events in a single <see cref="PartitionView{TEvent}"/>, invoking a provided processor for each event.
    /// </summary>
    /// <param name="view">The partition view containing events to iterate. If empty, the method returns <c>true</c> immediately.</param>
    /// <param name="state">Reference to the processing state that may be mutated by <paramref name="processor"/>.</param>
    /// <param name="processor">Callback invoked for each event that passes the optional <paramref name="filter"/>. If this callback returns <c>false</c>, processing stops and the method returns <c>false</c>.</param>
    /// <param name="filter">Optional predicate to determine whether an event should be processed. If <c>null</c>, all events are processed.</param>
    /// <param name="timestampSelector">Optional selector used to obtain an event timestamp passed to <paramref name="filter"/> and <paramref name="processor"/>.</param>
    /// <returns><c>true</c> if processing completed for the view (no early termination); <c>false</c> if the processor requested early termination.</returns>
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

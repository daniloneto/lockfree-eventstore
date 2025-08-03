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

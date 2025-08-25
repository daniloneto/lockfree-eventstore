using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Specialized high-performance ring buffer for Event struct with contiguous memory layout.
/// Optimized for cache locality and zero-allocation operations.
/// </summary>
public sealed class LockFreeEventRingBuffer
{
    private readonly Event[] _buffer; // Contiguous Event[] for optimal cache locality
    private long _head;
    private long _tail;
    private readonly Action<Event>? _onItemDiscarded;

    // Epoch-based consistency for snapshot operations
    private int _epoch;

    /// <summary>
    /// Initializes a new instance with the specified capacity.
    /// <summary>
    /// Initializes a new instance of <see cref="LockFreeEventRingBuffer"/> with the specified capacity.
    /// </summary>
    /// <param name="capacity">Positive number of slots to allocate for the ring buffer.</param>
    /// <param name="onItemDiscarded">Optional callback invoked when an item is overwritten due to capacity pressure.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="capacity"/> is less than or equal to zero.</exception>
    public LockFreeEventRingBuffer(int capacity, Action<Event>? onItemDiscarded = null)
    {
        // CA1512: prefer guard method
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
        _buffer = new Event[capacity]; // Contiguous memory allocation
        _onItemDiscarded = onItemDiscarded;
        _epoch = 0;
    }

    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => Math.Max(0, Math.Min(Capacity, Volatile.Read(ref _tail) - Volatile.Read(ref _head)));

    /// <summary>
    /// Whether the buffer is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Whether the buffer is at full capacity (approximate).
    /// </summary>
    public bool IsFull => CountApprox >= Capacity;

    /// <summary>
    /// Enqueues a single event, overwriting the oldest if necessary.
    /// <summary>
    /// Atomically appends an <see cref="Event"/> to the ring buffer. If the buffer is full this
    /// will overwrite the oldest item and, if configured, invoke the discard callback for the
    /// overwritten event.
    /// </summary>
    /// <param name="item">The event to enqueue.</param>
    /// <returns>Always returns <c>true</c> after the item has been placed into the buffer.</returns>
    /// <remarks>
    /// This method is lock-free and safe to call concurrently. It advances the internal tail
    /// index and will attempt to advance the head to maintain the ring invariant when overwrites occur.
    /// The internal epoch counter is incremented infrequently (once every 256 enqueues) to assist
    /// with snapshot consistency mechanisms.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(Event item)
    {
        var tail = Interlocked.Increment(ref _tail);
        var index = (int)((tail - 1) % Capacity);

        // Check if we're overwriting an existing event
        var currentHead = Volatile.Read(ref _head);
        if (tail - currentHead > Capacity)
        {
            var overwrittenIndex = (int)(currentHead % Capacity);
            _onItemDiscarded?.Invoke(_buffer[overwrittenIndex]);
        }

        _buffer[index] = item; // Direct struct assignment to contiguous array
        AdvanceHeadIfNeeded(tail);

        // Update epoch sparingly - only when tail advances significantly
        if ((tail & 0xFF) == 0) // Every 256 items
        {
            _ = Interlocked.Increment(ref _epoch);
        }

        return true;
    }

    /// <summary>
    /// Enqueues a batch of events with optimized epoch updating.
    /// Only updates epoch once for the entire batch instead of per item.
    /// <summary>
    /// Atomically reserves space and appends a contiguous batch of events into the ring buffer.
    /// </summary>
    /// <remarks>
    /// This method reserves space for the entire <paramref name="batch"/> in a single atomic operation and then writes all items into their reserved slots.
    /// If the enqueue causes older entries to be overwritten, the optional discard callback provided to the buffer constructor will be invoked for each overwritten item.
    /// The buffer head may be advanced to maintain the ring invariant and the internal epoch is incremented once for the whole batch.
    /// The operation is non-blocking and performs no allocation.
    /// </remarks>
    /// <param name="batch">The sequence of events to append. If empty, the method returns 0 and does nothing.</param>
    /// <returns>The number of events enqueued (equal to <paramref name="batch"/>.Length when non-empty, or 0 if <paramref name="batch"/> is empty).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueueBatch(ReadOnlySpan<Event> batch)
    {
        if (batch.IsEmpty)
        {
            return 0;
        }

        // Reserve space for the entire batch atomically
        var startTail = Interlocked.Add(ref _tail, batch.Length);
        var endTail = startTail;

        // Check for overwrites before writing
        var currentHead = Volatile.Read(ref _head);
        var overwriteStart = Math.Max(0, endTail - Capacity);

        if (_onItemDiscarded != null && overwriteStart > currentHead)
        {
            for (var i = currentHead; i < overwriteStart; i++)
            {
                var overwrittenIndex = (int)(i % Capacity);
                _onItemDiscarded(_buffer[overwrittenIndex]);
            }
        }

        // Write all events to their reserved positions
        for (var i = 0; i < batch.Length; i++)
        {
            var position = startTail - batch.Length + i;
            _buffer[(int)(position % Capacity)] = batch[i];
        }

        // Advance head if necessary to maintain ring buffer invariant
        var newHead = Math.Max(currentHead, endTail - Capacity);
        if (newHead > currentHead)
        {
            Volatile.Write(ref _head, newHead);
        }

        // Update epoch once for the entire batch
        _ = Interlocked.Increment(ref _epoch);

        return batch.Length;
    }

    /// <summary>
    /// Gets a snapshot of current events without enumeration allocation.
    /// <summary>
    /// Returns a point-in-time snapshot of the buffer's contents as an ordered sequence from oldest to newest.
    /// </summary>
    /// <remarks>
    /// The snapshot reflects the state at the time of call by reading the ring's head and tail; subsequent concurrent modifications are not reflected. The returned <see cref="IEnumerable{Event}"/> is backed by a newly allocated <see cref="List{Event}"/> containing at most <see cref="Capacity"/> items.
    /// </remarks>
    public IEnumerable<Event> EnumerateSnapshot()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(Capacity, tail - head);

        var results = new List<Event>((int)count);
        for (long i = 0; i < count; i++)
        {
            var index = (int)((head + i) % Capacity);
            results.Add(_buffer[index]);
        }
        return results;
    }

    /// <summary>
    /// Enumerates events with a filter predicate.
    /// <summary>
    /// Returns a point-in-time snapshot of events that satisfy the provided predicate.
    /// </summary>
    /// <param name="predicate">Predicate applied to each event; events for which this returns true are included.</param>
    /// <returns>A new list containing matching events in buffer order representing the snapshot at the time of the call.</returns>
    public IEnumerable<Event> EnumerateSnapshot(Func<Event, bool> predicate)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(Capacity, tail - head);

        var results = new List<Event>();
        for (long i = 0; i < count; i++)
        {
            var index = (int)((head + i) % Capacity);
            var item = _buffer[index];
            if (predicate(item))
            {
                results.Add(item);
            }
        }
        return results;
    }

    /// <summary>
    /// Enumerates events within a time range.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(long fromTicks, long toTicks)
    {
        return EnumerateSnapshot(e => e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks);
    }

    /// <summary>
    /// Enumerates events for a specific key.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(KeyId key)
    {
        return EnumerateSnapshot(e => e.Key.Equals(key));
    }

    /// <summary>
    /// Enumerates events for a specific key within a time range.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(KeyId key, long fromTicks, long toTicks)
    {
        return EnumerateSnapshot(e => e.Key.Equals(key) && e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks);
    }

    /// <summary>
    /// Purges events older than the specified timestamp.
    /// Returns the number of events purged.
    /// <summary>
    /// Removes (advances past) all events with TimestampTicks earlier than <paramref name="beforeTimestamp"/> and returns how many were removed.
    /// </summary>
    /// <param name="beforeTimestamp">Exclusive upper bound; events with TimestampTicks &lt; this value are purged.</param>
    /// <returns>The number of events removed from the buffer.</returns>
    /// <remarks>
    /// If any events are purged the buffer head is advanced and the internal epoch is incremented to signal a state change.
    /// The scan stops at the first non-matching event (the method assumes events are roughly time-ordered within the current window).
    /// </remarks>
    public long Purge(long beforeTimestamp)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(Capacity, tail - head);

        long purged = 0;
        var newHead = head;

        // Find the first event that should be kept
        for (long i = 0; i < count; i++)
        {
            var index = (int)((head + i) % Capacity);
            var item = _buffer[index];

            if (item.TimestampTicks < beforeTimestamp)
            {
                purged++;
                newHead = head + i + 1;
            }
            else
            {
                break; // Assuming events are roughly ordered by time
            }
        }

        if (purged > 0)
        {
            Volatile.Write(ref _head, newHead);
            _ = Interlocked.Increment(ref _epoch);
        }

        return purged;
    }

    /// <summary>
    /// Advances the ring buffer head if the provided tail index would otherwise make the buffer exceed its capacity.
    /// </summary>
    /// <param name="newTail">The new tail index (exclusive write position) used to compute the minimum allowed head: <c>newTail - Capacity</c>.</param>
    /// <remarks>
    /// Performs a best-effort, lock-free update: if the computed target head is strictly greater than the current head,
    /// the method attempts an atomic compare-and-swap to move <c>_head</c> to the target value. If another thread advances
    /// the head first, this call has no effect.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdvanceHeadIfNeeded(long newTail)
    {
        var currentHead = Volatile.Read(ref _head);
        var targetHead = newTail - Capacity;

        if (targetHead > currentHead)
        {
            // Try to advance head, but only if another thread hasn't already done it
            _ = Interlocked.CompareExchange(ref _head, targetHead, currentHead);
        }
    }

    /// <summary>
    /// Gets buffer statistics for debugging and monitoring.
    /// <summary>
    /// Reads and returns a snapshot of the buffer's current positional statistics.
    /// </summary>
    /// <returns>
    /// A tuple containing:
    /// Head — the index of the oldest item (volatile read);
    /// Tail — the next write index (volatile read);
    /// Epoch — the current epoch counter (volatile read);
    /// Count — the approximate number of items currently stored (clamped to [0, Capacity]).
    /// </returns>
    public (long Head, long Tail, int Epoch, long Count) GetStatistics()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var epoch = Volatile.Read(ref _epoch);
        var count = Math.Max(0, Math.Min(Capacity, tail - head));

        return (head, tail, epoch, count);
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// <summary>
    /// Creates a point-in-time, zero-allocation snapshot of the buffer and invokes <paramref name="processor"/> for each chunk of events.
    /// </summary>
    /// <remarks>
    /// The snapshot covers up to <see cref="Capacity"/> most recent events between the current head and tail at the time of calling.
    /// Events are copied into a rented array in chunks and passed to <paramref name="processor"/> as a <see cref="ReadOnlySpan{Event}"/>.
    /// The method does not allocate per-item memory; it uses the shared event pool and may invoke <paramref name="processor"/> multiple times.
    /// The final invocation may contain fewer than <paramref name="chunkSize"/> items.
    /// </remarks>
    /// <param name="processor">Action to process each populated span of events.</param>
    /// <param name="chunkSize">Maximum number of events provided to <paramref name="processor"/> per invocation. Defaults to <c>Buffers.DefaultChunkSize</c>.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(Capacity, tail - head);

        if (count == 0)
        {
            return;
        }

        Buffers.WithRentedBuffer<Event>(Math.Min(chunkSize, (int)count), buffer =>
        {
            var bufferCount = 0;
            // Respect requested chunk size even if pool returns larger arrays
            var effectiveChunk = Math.Min(chunkSize, buffer.Length);

            for (long i = 0; i < count; i++)
            {
                var index = (int)((head + i) % Capacity);
                buffer[bufferCount++] = _buffer[index];

                if (bufferCount >= effectiveChunk)
                {
                    processor(buffer.AsSpan(0, bufferCount));
                    bufferCount = 0;
                }
            }

            // Process remaining events
            if (bufferCount > 0)
            {
                processor(buffer.AsSpan(0, bufferCount));
            }
        }, Buffers.EventPool);
    }

    /// <summary>
    /// Zero-allocation filtered snapshot using chunked processing.
    /// <summary>
    /// Performs a point-in-time, zero-allocation snapshot of stored events that match the given predicate and invokes a processor for those events in contiguous chunks.
    /// </summary>
    /// <param name="predicate">Filter applied to each event; only matching events are buffered and passed to <paramref name="processor"/>.</param>
    /// <param name="processor">Called one or more times with a <see cref="ReadOnlySpan{Event}"/> containing up to <paramref name="chunkSize"/> matching events in chronological order.</param>
    /// <param name="chunkSize">Maximum number of matching events provided to <paramref name="processor"/> per invocation (defaults to <c>Buffers.DefaultChunkSize</c>).</param>
    /// <remarks>
    /// The method captures a snapshot of the current head/tail window and processes events from oldest to newest without allocating intermediate collections.
    /// If there are no events in the snapshot, the method returns immediately. The processor may be invoked multiple times; each invocation receives a contiguous span of matching events.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotFilteredZeroAlloc(Func<Event, bool> predicate, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(Capacity, tail - head);

        if (count == 0)
        {
            return;
        }

        Buffers.WithRentedBuffer<Event>(chunkSize, buffer =>
        {
            var bufferCount = 0;
            var effectiveChunk = Math.Min(chunkSize, buffer.Length);

            for (long i = 0; i < count; i++)
            {
                var index = (int)((head + i) % Capacity);
                var item = _buffer[index];

                if (predicate(item))
                {
                    buffer[bufferCount++] = item;

                    if (bufferCount >= effectiveChunk)
                    {
                        processor(buffer.AsSpan(0, bufferCount));
                        bufferCount = 0;
                    }
                }
            }

            // Process remaining events
            if (bufferCount > 0)
            {
                processor(buffer.AsSpan(0, bufferCount));
            }
        }, Buffers.EventPool);
    }

    /// <summary>
    /// Zero-allocation time range snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotTimeRangeZeroAlloc(long fromTicks, long toTicks, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        SnapshotFilteredZeroAlloc(e => e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks, processor, chunkSize);
    }

    /// <summary>
    /// Zero-allocation key-specific snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotByKeyZeroAlloc(KeyId key, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        SnapshotFilteredZeroAlloc(e => e.Key.Equals(key), processor, chunkSize);
    }

    /// <summary>
    /// Zero-allocation key and time range snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotByKeyAndTimeZeroAlloc(KeyId key, long fromTicks, long toTicks, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        SnapshotFilteredZeroAlloc(e => e.Key.Equals(key) && e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks, processor, chunkSize);
    }
}

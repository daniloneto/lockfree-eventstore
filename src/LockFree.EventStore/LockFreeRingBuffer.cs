using System.Threading;
using System.Runtime.CompilerServices;
using System.Buffers;

namespace LockFree.EventStore;

/// <summary>
/// Fixed-size MPMC ring buffer with lock-free overwrite when full.
/// </summary>
public sealed class LockFreeRingBuffer<T>
{
    private readonly T[] _buffer;
    private long _head;
    private long _tail;
    private readonly Action<T>? _onItemDiscarded;
      // Epoch-based consistency for snapshot operations
    private int _epoch;

    /// <summary>
    /// Initializes a new instance with the specified capacity.
    /// <summary>
    /// Initializes a new fixed-capacity lock-free ring buffer that overwrites the oldest items when full.
    /// </summary>
    /// <param name="capacity">The fixed maximum number of items the buffer can hold; must be greater than zero.</param>
    /// <param name="onItemDiscarded">Optional callback invoked once for each item that is overwritten/removed due to capacity pressure.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="capacity"/> is less than or equal to zero.</exception>
    public LockFreeRingBuffer(int capacity, Action<T>? onItemDiscarded = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
        _buffer = new T[capacity];
        _onItemDiscarded = onItemDiscarded;
        _epoch = 0;
    }    /// <summary>
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
    public bool IsFull => CountApprox >= Capacity;    /// <summary>
    /// Enqueues a single item, overwriting the oldest if necessary.
    /// <summary>
    /// Atomically enqueues a single item into the ring buffer in a lock-free, multi-producer/multi-consumer manner.
    /// </summary>
    /// <remarks>
    /// This method reserves a slot by advancing the internal tail index, writes the item into the buffer,
    /// and advances the head as needed to maintain the fixed-capacity invariant (oldest items are overwritten when full).
    /// When items are discarded due to overwrite, the optional discard callback (if provided) may be invoked by AdvanceHeadTo.
    /// The method periodically updates an internal epoch counter for consistent snapshot/view operations.
    /// </remarks>
    /// <returns>Always returns true after writing the item.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T item)
    {
        var tail = Interlocked.Increment(ref _tail);
        var index = (int)((tail - 1) % Capacity);
        _buffer[index] = item;
        AdvanceHeadTo(tail);
        
        // Update epoch sparingly - only when tail advances significantly
        if ((tail & 0xFF) == 0) // Every 256 items
        {
            _ = Interlocked.Increment(ref _epoch);
        }
        
        return true;
    }/// <summary>
    /// Enqueues a batch of items.
    /// <summary>
    /// Enqueues each item from the provided span into the ring buffer.
    /// </summary>
    /// <param name="batch">Span of items to enqueue. Each element is enqueued individually and may overwrite the oldest items if the buffer is full.</param>
    /// <returns>The number of items processed and enqueued (equal to <c>batch.Length</c>).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueue(ReadOnlySpan<T> batch)
    {
        int written = 0;
        foreach (var item in batch)
        {
            _ = TryEnqueue(item);
            written++;
        }
        return written;
    }

    /// <summary>
    /// Enqueues a batch of items with optimized epoch updating.
    /// Only updates epoch once for the entire batch instead of per item.
    /// <summary>
    /// Atomically enqueues all items from <paramref name="batch"/> into the ring buffer.
    /// Reserves space for the whole batch in a single atomic operation, writes items into their reserved slots,
    /// advances the head to preserve capacity (overwriting and triggering discard callbacks for any overwritten items),
    /// and updates the internal epoch once for the batch.
    /// </summary>
    /// <param name="batch">The sequence of items to enqueue. If empty, nothing is written and 0 is returned.</param>
    /// <returns>The number of items written (equal to <c>batch.Length</c>, or 0 if <paramref name="batch"/> is empty).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueueBatch(ReadOnlySpan<T> batch)
    {
        if (batch.IsEmpty)
        {
            return 0;
        }

        // Reserve space for the entire batch atomically
        var endTail = Interlocked.Add(ref _tail, batch.Length);
        var startPosition = endTail - batch.Length;
        
        // Write all items to their reserved positions
        for (int i = 0; i < batch.Length; i++)
        {
            var position = startPosition + i;
            _buffer[position % Capacity] = batch[i];
        }
        
        // Advance head to maintain ring invariants (and invoke discard callbacks for all overwritten)
        AdvanceHeadTo(endTail);
        
        // Update epoch once for the batch
        if ((endTail & 0xFF) == 0 || ((endTail - batch.Length) & 0xFF) > (endTail & 0xFF))
        {
            _ = Interlocked.Increment(ref _epoch);
        }
        
        return batch.Length;
    }    /// <summary>
    /// Advances the ring buffer head so the distance from head to <paramref name="tail"/> does not exceed the buffer Capacity.
    /// </summary>
    /// <remarks>
    /// Performs a compare-and-swap loop to atomically move <c>_head</c> forward to <c>tail - Capacity</c> when necessary.
    /// If a discard callback was provided, it is invoked once for each item that becomes overwritten as the head advances.
    /// </remarks>
    /// <param name="tail">The (exclusive) tail index to which the head should be advanced relative to the buffer Capacity.</param>
    private void AdvanceHeadTo(long tail)
    {
        while (true)
        {
            var head = Volatile.Read(ref _head);
            var desiredHead = tail - Capacity;
            if (desiredHead <= head)
            {
                break;
            }

            // Try to advance head to the desired position in one step
            if (Interlocked.CompareExchange(ref _head, desiredHead, head) == head)
            {
                // Notify about each discarded item if callback is provided
                if (_onItemDiscarded != null)
                {
                    for (long h = head; h < desiredHead; h++)
                    {
                        var idx = (int)(h % Capacity);
                        var discardedItem = _buffer[idx];
                        _onItemDiscarded(discardedItem);
                    }
                }
                break;
            }
        }
    }

    /// <summary>
    /// Clears all items from the buffer.
    /// <summary>
    /// Clears all items from the ring buffer and resets head and tail indices to zero.
    /// </summary>
    /// <remarks>
    /// Any items currently stored between the volatile head and tail pointers are set to their default value
    /// to assist garbage collection. Head and tail are then reset with volatile writes.
    /// This method does not attempt to coordinate with concurrent producers/consumers; callers should ensure
    /// appropriate synchronization if other threads may be accessing the buffer concurrently.
    /// </remarks>
    public void Clear()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        // Clear the array to help GC
        if (head < tail)
        {
            for (long i = head; i < tail; i++)
            {
                _buffer[i % Capacity] = default(T)!;
            }
        }
        
        // Reset pointers
        Volatile.Write(ref _head, 0);
        Volatile.Write(ref _tail, 0);
    }    /// <summary>
    /// Copies a snapshot of the buffer into <paramref name="destination"/>.
    /// </summary>
    /// <summary>
    /// Copies a best-effort, point-in-time snapshot of the buffer into the provided destination span.
    /// </summary>
    /// <param name="destination">Span to receive items; only the first <c>min(destination.Length, Count)</c> elements are written.</param>
    /// <returns>The number of items copied into <paramref name="destination"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Snapshot(Span<T> destination)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var length = (int)Math.Min(Math.Min(tail - head, Capacity), destination.Length);
        
        if (length <= 0)
        {
            return 0;
        }

        var startIndex = (int)(head % Capacity);
        
        // Check if we need to handle wrap-around
        if (startIndex + length <= Capacity)
        {
            // Single segment copy - can use fast span copy
            var source = _buffer.AsSpan(startIndex, length);
            source.CopyTo(destination);
        }
        else
        {
            // Two-segment copy for wrap-around
            var firstSegmentLength = Capacity - startIndex;
            var secondSegmentLength = length - firstSegmentLength;
            
            var firstSource = _buffer.AsSpan(startIndex, firstSegmentLength);
            var secondSource = _buffer.AsSpan(0, secondSegmentLength);
            
            firstSource.CopyTo(destination);
            secondSource.CopyTo(destination.Slice(firstSegmentLength));
        }
        
        return length;
    }/// <summary>
    /// Convenience method that allocates an array and returns an enumerable.
    /// <summary>
    /// Returns a point-in-time snapshot of the buffer's contents in oldest-to-newest order.
    /// </summary>
    /// <returns>A newly allocated <see cref="IEnumerable{T}"/> containing the items present in the buffer at the time of the call (oldest first).</returns>
    public IEnumerable<T> EnumerateSnapshot()
    {
        var tmp = new T[Capacity];
        var len = Snapshot(tmp);
        var results = new List<T>(len);
        for (int i = 0; i < len; i++)
        {
            results.Add(tmp[i]);
        }

        return results;
    }

    /// <summary>
    /// Creates a view of the buffer contents without copying data.
    /// Uses epoch-based consistency to ensure a stable snapshot.
    /// <summary>
    /// Creates a consistent, read-only view of the buffer's current contents.
    /// </summary>
    /// <remarks>
    /// Attempts an epoch-validated read up to a limited number of retries to obtain a consistent head/tail snapshot. If a consistent snapshot cannot be obtained within the retry limit, a best-effort (potentially slightly inconsistent) view is returned.
    /// </remarks>
    /// <param name="timestampSelector">Optional selector used by the returned view to provide per-item timestamps; when null the view will not include timestamps.</param>
    /// <returns>A <see cref="PartitionView{T}"/> representing the buffer contents at the captured range (may be a single or two-segment view to account for wrap-around).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<T> CreateView(IEventTimestampSelector<T>? timestampSelector = null)
    {
        const int maxRetries = 10;
        
        for (int retry = 0; retry < maxRetries; retry++)
        {
            var startEpoch = Volatile.Read(ref _epoch);
            var head = Volatile.Read(ref _head);
            var tail = Volatile.Read(ref _tail);
            var endEpoch = Volatile.Read(ref _epoch);
            
            // Check if epoch changed during read - if so, retry
            if (startEpoch != endEpoch)
            {
                continue;
            }

            return CreateViewFromRange(head, tail, timestampSelector);
        }
        
        // Fallback: take current snapshot even if not perfectly consistent
        var currentHead = Volatile.Read(ref _head);
        var currentTail = Volatile.Read(ref _tail);
        return CreateViewFromRange(currentHead, currentTail, timestampSelector);
    }

    /// <summary>
    /// Creates a view with explicit timestamp filtering.
    /// <summary>
    /// Creates a view of the buffer containing only items whose timestamps fall within the specified time window.
    /// </summary>
    /// <param name="fromTicks">Lower bound of the time window (in ticks).</param>
    /// <param name="toTicks">Upper bound of the time window (in ticks).</param>
    /// <param name="timestampSelector">Selector used to extract event timestamps from items. Cannot be null.</param>
    /// <returns>
    /// A <see cref="PartitionView{T}"/> representing the items in the buffer whose timestamps are within the given window.
    /// The returned view may consist of one or two segments to account for ring wrap-around.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="timestampSelector"/> is null.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<T> CreateViewFiltered(
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T> timestampSelector)
    {
        ArgumentNullException.ThrowIfNull(timestampSelector);
            
        const int maxRetries = 10;
        
        for (int retry = 0; retry < maxRetries; retry++)
        {
            var startEpoch = Volatile.Read(ref _epoch);
            var head = Volatile.Read(ref _head);
            var tail = Volatile.Read(ref _tail);
            var endEpoch = Volatile.Read(ref _epoch);
            
            if (startEpoch != endEpoch)
            {
                continue;
            }

            return CreateFilteredViewFromRange(head, tail, fromTicks, toTicks, timestampSelector);
        }
        
        // Fallback
        var currentHead = Volatile.Read(ref _head);
        var currentTail = Volatile.Read(ref _tail);
        return CreateFilteredViewFromRange(currentHead, currentTail, fromTicks, toTicks, timestampSelector);
    }

    /// <summary>
    /// Creates a consistent PartitionView over the items in the buffer between the provided head and tail indices.
    /// </summary>
    /// <param name="head">The absolute index of the oldest item in the desired range.</param>
    /// <param name="tail">The absolute index one past the newest item in the desired range.</param>
    /// <param name="timestampSelector">
    /// Optional selector to extract timestamps from items; when provided the view's from/to tick bounds are set
    /// from the first and last items in the view.
    /// </param>
    /// <returns>
    /// A PartitionView containing one or two memory segments that represent the contiguous elements in the ring
    /// buffer between <paramref name="head"/> (inclusive) and <paramref name="tail"/> (exclusive).
    /// If the computed count is zero or negative the returned view contains empty segments and zero counts.
    /// The count is clamped to the buffer Capacity and to Int32.MaxValue.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateViewFromRange(long head, long tail, IEventTimestampSelector<T>? timestampSelector)
    {
        var count = (int)Math.Min(Math.Min(tail - head, Capacity), int.MaxValue);
        if (count <= 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, 0, 0);
        }
        
        var startIndex = (int)(head % Capacity);
        var endIndex = (int)((head + count - 1) % Capacity);
        
        long fromTicks = 0, toTicks = 0;
        if (timestampSelector != null && count > 0)
        {
            var firstEvent = _buffer[startIndex];
            var lastEvent = _buffer[endIndex];
            fromTicks = timestampSelector.GetTimestamp(firstEvent).Ticks;
            toTicks = timestampSelector.GetTimestamp(lastEvent).Ticks;
        }
        
        // Check if we need to wrap around
        if (startIndex + count <= Capacity)
        {
            // Single segment case
            var segment = _buffer.AsMemory(startIndex, count);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, count, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case: two segments
            var firstSegmentLength = Capacity - startIndex;
            var secondSegmentLength = count - firstSegmentLength;
            
            var segment1 = _buffer.AsMemory(startIndex, firstSegmentLength);
            var segment2 = _buffer.AsMemory(0, secondSegmentLength);
            
            return new PartitionView<T>(segment1, segment2, count, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Builds a time-filtered PartitionView over the items in the range [head, tail),
    /// returning only items whose event timestamps fall within the inclusive window [fromTicks, toTicks].
    /// </summary>
    /// <param name="head">Sequence index of the oldest item in the buffer (inclusive).</param>
    /// <param name="tail">Sequence index of the next write position (exclusive).</param>
    /// <param name="fromTicks">Inclusive lower bound of the allowed timestamp window (ticks).</param>
    /// <param name="toTicks">Inclusive upper bound of the allowed timestamp window (ticks).</param>
    /// <param name="timestampSelector">Selector that extracts the timestamp from an item.</param>
    /// <returns>
    /// A PartitionView&lt;T&gt; describing zero, one, or two memory segments that contain the filtered items
    /// (accounts for buffer wrap-around). The view's Count equals the number of items whose timestamps
    /// are within [fromTicks, toTicks]. If no items match, both segments are empty and Count is 0.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateFilteredViewFromRange(
        long head, 
        long tail, 
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T> timestampSelector)
    {
        var totalCount = (int)Math.Min(Math.Min(tail - head, Capacity), int.MaxValue);
        if (totalCount <= 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, fromTicks, toTicks);
        }
        
        // Find the range of valid events
        int validStart = -1, validEnd = -1;
        int validCount = 0;
        
        for (int i = 0; i < totalCount; i++)
        {
            var index = (int)((head + i) % Capacity);
            var item = _buffer[index];            if (timestampSelector != null)
            {
                var timestamp = timestampSelector.GetTimestamp(item);
                var ticks = timestamp.Ticks;
                
                // Use same logic as WithinWindow: from inclusive, to inclusive  
                if (ticks >= fromTicks && ticks <= toTicks)
                {
                    if (validStart == -1)
                    {
                        validStart = i;
                    }

                    validEnd = i;
                    validCount++;
                }
            }
        }
        
        if (validCount == 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, fromTicks, toTicks);
        }
        
        // Create view for the valid range
        var actualStartIndex = (int)((head + validStart) % Capacity);
        var actualEndIndex = (int)((head + validEnd) % Capacity);
        
        if (actualStartIndex <= actualEndIndex)
        {
            // Single segment
            var length = actualEndIndex - actualStartIndex + 1;
            var segment = _buffer.AsMemory(actualStartIndex, length);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, validCount, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case
            var firstSegmentLength = Capacity - actualStartIndex;
            var secondSegmentLength = actualEndIndex + 1;
            
            var segment1 = _buffer.AsMemory(actualStartIndex, firstSegmentLength);
            var segment2 = _buffer.AsMemory(0, secondSegmentLength);
            
            return new PartitionView<T>(segment1, segment2, validCount, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Internal method for window aggregation. Enumerates items within a timestamp range
    /// without allocating collections, using a callback for each valid item.
    /// <summary>
    /// Iterates over the buffer's current window and invokes a callback for each item whose timestamp falls within the inclusive [fromTicks, toTicks] range. If no timestamp selector is provided, the callback is invoked for every item with ticks = 0.
    /// </summary>
    /// <param name="fromTicks">Inclusive lower bound of the timestamp window.</param>
    /// <param name="toTicks">Inclusive upper bound of the timestamp window.</param>
    /// <param name="timestampSelector">Optional selector used to extract an item's timestamp; when null no timestamp filtering is applied.</param>
    /// <param name="state">Mutable state passed by reference to the callback and updated in-place.</param>
    /// <param name="callback">Called for each selected item with the state (by reference), the item, and its timestamp ticks (or 0 if no selector).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void EnumerateWindow<TState>(
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T>? timestampSelector,
        ref TState state,
        WindowItemCallback<T, TState> callback)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, Capacity);
        
        for (long i = 0; i < count; i++)
        {
            var index = (head + i) % Capacity;
            var item = _buffer[index];            if (timestampSelector != null)
            {
                var timestamp = timestampSelector.GetTimestamp(item);
                var ticks = timestamp.Ticks;
                
                // Use same logic as WithinWindow: from inclusive, to inclusive  
                if (ticks >= fromTicks && ticks <= toTicks)
                {
                    callback(ref state, item, ticks);
                }
            }
            else
            {
                // No timestamp filtering - include all items
                callback(ref state, item, 0);
            }
        }
    }

    /// <summary>
    /// Advances the window head to start from the first event with timestamp >= windowStartTicks.
    /// Updates aggregate state by removing events that fall outside the window.
    /// <summary>
    /// Advances the logical window head to the first item with timestamp >= <paramref name="windowStartTicks"/>.
    /// </summary>
    /// <param name="windowStartTicks">The lower bound (inclusive) of the window in ticks. Items with timestamps less than this will be removed from the window.</param>
    /// <param name="timestampSelector">Used to obtain an item's timestamp. If null the method is a no-op.</param>
    /// <param name="state">Mutable state passed to <paramref name="removeCallback"/>; updated by each removal.</param>
    /// <param name="removeCallback">Called for every item removed from the window; receives the current state by reference, the item being removed, and its timestamp in ticks.</param>
    /// <param name="windowHeadIndex">Index (relative to the buffer's current head) of the first item currently in the window. On return this is advanced to the new window start.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void AdvanceWindowTo<TState>(
        long windowStartTicks,
        IEventTimestampSelector<T>? timestampSelector,
        ref TState state,
        WindowAdvanceCallback<T, TState> removeCallback,
        ref int windowHeadIndex)
    {
        if (timestampSelector == null)
        {
            return;
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, Capacity);
        
        // Find new window head position
        var newWindowHead = windowHeadIndex;
        
        for (long i = windowHeadIndex; i < count; i++)
        {
            var index = (head + i) % Capacity;
            var item = _buffer[index];
            var timestamp = timestampSelector.GetTimestamp(item);
            
            if (timestamp.Ticks >= windowStartTicks)
            {
                break; // Found the new window start
            }
            
            // Remove this item from the window state
            removeCallback(ref state, item, timestamp.Ticks);
            newWindowHead = (int)(i + 1);
        }
        
        windowHeadIndex = newWindowHead;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// <summary>
    /// Takes a snapshot of the buffer into a pooled array and invokes the provided processor on the snapshot in contiguous chunks without allocating additional buffers.
    /// </summary>
    /// <param name="processor">Called for each contiguous chunk of items; receives a ReadOnlySpan&lt;T&gt; over the snapshot data.</param>
    /// <param name="pool">The ArrayPool used to rent/return the temporary buffer.</param>
    /// <param name="chunkSize">Maximum chunk size passed to <paramref name="processor"/>. The rented buffer size is min(<paramref name="chunkSize"/>, Capacity).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotZeroAlloc<TBuffer>(Action<ReadOnlySpan<T>> processor, ArrayPool<T> pool, int chunkSize = Buffers.DefaultChunkSize)
    {
        var buffer = pool.Rent(Math.Min(chunkSize, Capacity));
        try
        {
            var len = Snapshot(buffer);
            if (len > 0)
            {
                // Process in chunks
                for (int i = 0; i < len; i += chunkSize)
                {
                    var chunkLen = Math.Min(chunkSize, len - i);
                    processor(buffer.AsSpan(i, chunkLen));
                }
            }
        }
        finally
        {
            pool.Return(buffer, clearArray: false);
        }
    }    /// <summary>
    /// Zero-allocation enumeration using callback processing.
    /// <summary>
    /// Iterates the buffer's current items without allocating, updating a caller-provided state via a processor function.
    /// </summary>
    /// <param name="initialState">Initial processor state passed to the first invocation.</param>
    /// <param name="processor">
    /// Function invoked for each item: receives the current state and the item, and returns a tuple with the updated state
    /// and a boolean indicating whether iteration should continue (true to continue, false to stop early).
    /// </param>
    /// <param name="finalState">The state produced after processing completes (or after early termination).</param>
    /// <remarks>
    /// The method reads the head and tail indexes once (volatile reads) and processes up to Min(tail - head, Capacity) items
    /// in FIFO order. Processing is zero-allocation and may stop early if the processor returns Continue=false.
    /// The snapshot is best-effort and may not reflect subsequent concurrent enqueues/discards.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ProcessItemsZeroAlloc<TState>(TState initialState, Func<TState, T, (TState State, bool Continue)> processor, out TState finalState)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, Capacity);
        
        var state = initialState;
        for (long i = 0; i < count; i++)
        {
            var index = (head + i) % Capacity;
            var item = _buffer[index];
            
            var result = processor(state, item);
            state = result.State;
            
            if (!result.Continue)
            {
                break; // Early termination
            }
        }
        
        finalState = state;
    }
}

/// <summary>
/// Callback delegate for processing items during window enumeration.
/// </summary>
internal delegate void WindowItemCallback<in T, TState>(ref TState state, T item, long ticks);

/// <summary>
/// Callback delegate for removing items during window advancement.
/// </summary>
internal delegate void WindowAdvanceCallback<in T, TState>(ref TState state, T item, long ticks);

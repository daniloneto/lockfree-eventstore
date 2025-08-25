using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// High-performance MPMC ring buffer with anti-false sharing padding.
/// Uses padded partition headers to prevent false sharing in multi-core scenarios.
/// </summary>
/// <typeparam name="T">The type of items stored in the buffer</typeparam>
public sealed class PaddedLockFreeRingBuffer<T>
{
    private readonly T[] _buffer;
    private readonly Action<T>? _onItemDiscarded;
    private PartitionHeader _header;
    
    /// <summary>
    /// Initializes a new instance with the specified capacity and anti-false sharing padding.
    /// </summary>
    public PaddedLockFreeRingBuffer(int capacity, Action<T>? onItemDiscarded = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        
        _buffer = new T[capacity];
        _onItemDiscarded = onItemDiscarded;
        _header = new PartitionHeader(capacity);
    }
    
    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity => _header.Capacity;

    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => _header.GetApproximateCount();

    /// <summary>
    /// Whether the buffer is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Whether the buffer is at full capacity (approximate).
    /// </summary>
    public bool IsFull => CountApprox >= Capacity;

    /// <summary>
    /// Enqueues a single item, overwriting the oldest if necessary.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T item)
    {
        var newTail = Interlocked.Increment(ref _header.Tail);
        var index = (newTail - 1) % Capacity;
        
        // Store the item
        _buffer[index] = item;
        
        // Update count and handle potential overwrite
        _header.UpdateCount();
        
        // Check if we need to advance head (buffer is full)
        var head = Volatile.Read(ref _header.Head);
        if (newTail - head > Capacity)
        {
            var oldItem = _buffer[head % Capacity];
            if (Interlocked.CompareExchange(ref _header.Head, head + 1, head) == head)
            {
                _onItemDiscarded?.Invoke(oldItem);
                _header.IncrementEpoch();
            }
        }
        
        return true;
    }

    /// <summary>
    /// Attempts to enqueue multiple items in a batch operation.
    /// <summary>
    /// Attempts to enqueue a batch of items into the ring buffer.
    /// </summary>
    /// <param name="items">Span of items to append to the buffer. May be empty.</param>
    /// <returns>The number of items actually enqueued (equal to <c>items.Length</c> when non-empty, otherwise 0).</returns>
    /// <remarks>
    /// This is a lock-free append that atomically reserves space by advancing the tail by the batch size, writes the items into the circular buffer, and updates the approximate count.
    /// If the incoming batch causes the tail to advance past the head by more than the buffer capacity, the oldest items are considered overwritten:
    /// - Up to <c>batchSize</c> oldest items will be discarded.
    /// - If an on-item-discard callback was provided to the buffer, it will be invoked for each discarded item.
    /// - When head is successfully advanced to drop discarded items, the internal epoch is incremented to signal the overwrite.
    /// </remarks>
    public int TryEnqueueBatch(ReadOnlySpan<T> items)
    {
        if (items.IsEmpty)
        {
            return 0;
        }

        var batchSize = items.Length;
        var newTail = Interlocked.Add(ref _header.Tail, batchSize);
        var startIndex = newTail - batchSize;
        
        // Store all items
        for (int i = 0; i < batchSize; i++)
        {
            var index = (startIndex + i) % Capacity;
            _buffer[index] = items[i];
        }
        
        // Update count
        _header.UpdateCount();
        
        // Handle potential overwrites
        var head = Volatile.Read(ref _header.Head);
        var overflow = newTail - head - Capacity;
        
        if (overflow > 0)
        {
            // We've overwritten some items
            var itemsToDiscard = Math.Min((int)overflow, batchSize);
            
            if (_onItemDiscarded != null)
            {
                for (int i = 0; i < itemsToDiscard; i++)
                {
                    var discardIndex = (head + i) % Capacity;
                    _onItemDiscarded(_buffer[discardIndex]);
                }
            }
            
            // Advance head past discarded items
            var newHead = head + itemsToDiscard;
            if (Interlocked.CompareExchange(ref _header.Head, newHead, head) == head)
            {
                _header.IncrementEpoch();
            }
        }
        
        return batchSize;
    }

    /// <summary>
    /// Takes a snapshot of current items into the provided buffer.
    /// <summary>
    /// Copies a snapshot of up to <paramref name="destination"/>.Length items from the ring buffer into <paramref name="destination"/>.
    /// </summary>
    /// <param name="destination">Span to receive items from the current buffer contents; only the first <c>n</c> elements are written, where <c>n</c> is the returned count.</param>
    /// <returns>The number of items copied into <paramref name="destination"/>. This is the minimum of the buffer capacity, the current item count (tail - head), and <paramref name="destination"/>.Length.</returns>
    public int Snapshot(Span<T> destination)
    {
        var head = Volatile.Read(ref _header.Head);
        var tail = Volatile.Read(ref _header.Tail);
        var count = (int)Math.Min(destination.Length, Math.Min(Capacity, tail - head));
        
        if (count <= 0)
        {
            return 0;
        }

        for (int i = 0; i < count; i++)
        {
            var index = (head + i) % Capacity;
            destination[i] = _buffer[index];
        }
        
        return count;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// </summary>
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
    }

    /// <summary>
    /// Zero-allocation enumeration using callback processing.
    /// <summary>
    /// Iterates the current contents of the ring buffer without allocations, applying a stateful processor to each item in order.
    /// </summary>
    /// <param name="initialState">Initial processor state supplied to the first invocation.</param>
    /// <param name="processor">Function invoked for each item; returns the updated state and a boolean indicating whether iteration should continue.</param>
    /// <param name="finalState">The processor state after iteration completes (either after processing all available items or after early termination).</param>
    /// <remarks>
    /// The method captures head and tail atomically at the start and processes up to min(tail - head, Capacity) items starting from head (wrap-aware).
    /// It does not modify the buffer or its head/tail pointers. Processing stops early if <paramref name="processor"/> returns Continue = false.
    /// This method avoids allocations and is intended for zero-allocation, in-place processing of buffer contents.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ProcessItemsZeroAlloc<TState>(TState initialState, Func<TState, T, (TState State, bool Continue)> processor, out TState finalState)
    {
        var head = Volatile.Read(ref _header.Head);
        var tail = Volatile.Read(ref _header.Tail);
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

    /// <summary>
    /// Creates a view of the buffer contents without copying data.
    /// Uses epoch-based consistency to ensure a stable snapshot.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<T> CreateView(IEventTimestampSelector<T>? timestampSelector = null)
    {
        // Try multiple times to get a consistent snapshot
        for (int attempt = 0; attempt < 3; attempt++)
        {
            var epoch1 = Volatile.Read(ref _header.Epoch);
            var head = Volatile.Read(ref _header.Head);
            var tail = Volatile.Read(ref _header.Tail);
            var epoch2 = Volatile.Read(ref _header.Epoch);
            
            if (epoch1 == epoch2)
            {
                return CreateViewFromRange(head, tail, timestampSelector);
            }
        }
        
        // Fallback: take current snapshot even if not perfectly consistent
        var currentHead = Volatile.Read(ref _header.Head);
        var currentTail = Volatile.Read(ref _header.Tail);
        return CreateViewFromRange(currentHead, currentTail, timestampSelector);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateViewFromRange(long head, long tail, IEventTimestampSelector<T>? timestampSelector)
    {
        var count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, 0, 0);
        }

        var headIndex = (int)(head % Capacity);
        var tailIndex = (int)(tail % Capacity);
        
        // Calculate timestamp range if selector is provided
        long fromTicks = 0, toTicks = 0;
        if (timestampSelector != null && count > 0)
        {
            var firstItem = _buffer[headIndex];
            var lastItem = _buffer[(tailIndex - 1 + Capacity) % Capacity];
            fromTicks = timestampSelector.GetTimestamp(firstItem).Ticks;
            toTicks = timestampSelector.GetTimestamp(lastItem).Ticks;
        }

        if (tailIndex > headIndex || (tailIndex == headIndex && count == Capacity))
        {
            // No wrap-around case
            var segment = new ReadOnlyMemory<T>(_buffer, headIndex, count);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, count, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case
            var segment1 = new ReadOnlyMemory<T>(_buffer, headIndex, Capacity - headIndex);
            var segment2 = new ReadOnlyMemory<T>(_buffer, 0, tailIndex);
            return new PartitionView<T>(segment1, segment2, count, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Creates a filtered view of buffer contents based on timestamp range.
    /// <summary>
    /// Creates a partition view containing only items whose timestamps fall within the inclusive range [fromTicks, toTicks].
    /// </summary>
    /// <param name="fromTicks">Inclusive lower bound of the timestamp filter.</param>
    /// <param name="toTicks">Inclusive upper bound of the timestamp filter.</param>
    /// <param name="timestampSelector">Selector used to obtain timestamps from items.</param>
    /// <returns>
    /// A <see cref="PartitionView{T}"/> representing the filtered view of the buffer at a (near-)consistent point in time.
    /// The method attempts up to 10 epoch-consistent reads to avoid copying; if consistency cannot be achieved it falls back to a current head/tail snapshot.
    /// </returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="timestampSelector"/> is null.</exception>
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
            var startEpoch = Volatile.Read(ref _header.Epoch);
            var head = Volatile.Read(ref _header.Head);
            var tail = Volatile.Read(ref _header.Tail);
            var endEpoch = Volatile.Read(ref _header.Epoch);
            
            if (startEpoch != endEpoch)
            {
                continue;
            }

            return CreateFilteredViewFromRange(head, tail, fromTicks, toTicks, timestampSelector);
        }
        
        // Fallback
        var currentHead = Volatile.Read(ref _header.Head);
        var currentTail = Volatile.Read(ref _header.Tail);
        return CreateFilteredViewFromRange(currentHead, currentTail, fromTicks, toTicks, timestampSelector);
    }

    /// <summary>
    /// Builds a PartitionView containing the items whose timestamps (via <paramref name="timestampSelector"/>) fall within the inclusive range [<paramref name="fromTicks"/>, <paramref name="toTicks"/>]
    /// from the logical range [<paramref name="head"/>, <paramref name="tail"/>). The returned view references the internal buffer (no copies) and may be one or two segments when the range wraps.
    /// </summary>
    /// <param name="head">Absolute sequence index of the first available item in the partition.</param>
    /// <param name="tail">Absolute sequence index one past the last available item in the partition.</param>
    /// <param name="fromTicks">Inclusive lower bound of the timestamp range to include.</param>
    /// <param name="toTicks">Inclusive upper bound of the timestamp range to include.</param>
    /// <param name="timestampSelector">Selector used to extract an event timestamp from each item.</param>
    /// <returns>
    /// A PartitionView containing only the items whose timestamps are within [fromTicks, toTicks]. If no items match, returns an empty view. The view's count is the number of matching items and the view references the buffer segments directly.
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
            var item = _buffer[index];
            var itemTicks = timestampSelector.GetTimestamp(item).Ticks;
            
            if (itemTicks >= fromTicks && itemTicks <= toTicks)
            {
                if (validStart == -1)
                {
                    validStart = i;
                }

                validEnd = i;
                validCount++;
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
            var segment = new ReadOnlyMemory<T>(_buffer, actualStartIndex, length);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, validCount, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case
            var firstSegmentLength = Capacity - actualStartIndex;
            var secondSegmentLength = actualEndIndex + 1;
            
            var segment1 = new ReadOnlyMemory<T>(_buffer, actualStartIndex, firstSegmentLength);
            var segment2 = new ReadOnlyMemory<T>(_buffer, 0, secondSegmentLength);
            
            return new PartitionView<T>(segment1, segment2, validCount, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Clears all items from the buffer.
    /// </summary>
    public void Clear()
    {
        var tail = Volatile.Read(ref _header.Tail);
        Volatile.Write(ref _header.Head, tail);
        _header.UpdateCount();
        _header.IncrementEpoch();
    }

    /// <summary>
    /// Gets statistics for monitoring and debugging.
    /// </summary>
    public (long Head, long Tail, int Epoch, long Count) GetStatistics()
    {
        var head = Volatile.Read(ref _header.Head);
        var tail = Volatile.Read(ref _header.Tail);
        var epoch = Volatile.Read(ref _header.Epoch);
        var count = _header.GetApproximateCount();
        
        return (head, tail, epoch, count);
    }

    /// <summary>
    /// Convenience method that allocates an array and returns an enumerable.
    /// <summary>
    /// Returns a new list containing a point-in-time snapshot of up to <see cref="Capacity"/> items currently stored in the buffer.
    /// </summary>
    /// <returns>A new <see cref="IEnumerable{T}"/> (concrete <see cref="List{T}"/>) containing the captured items; count is between 0 and <see cref="Capacity"/>.</returns>
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
}

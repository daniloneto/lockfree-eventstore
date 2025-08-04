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
    private readonly int _capacity;
    private long _head;
    private long _tail;
    private readonly Action<T>? _onItemDiscarded;
      // Epoch-based consistency for snapshot operations
    private int _epoch;

    /// <summary>
    /// Initializes a new instance with the specified capacity.
    /// </summary>
    public LockFreeRingBuffer(int capacity, Action<T>? onItemDiscarded = null)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        _capacity = capacity;
        _buffer = new T[capacity];
        _onItemDiscarded = onItemDiscarded;
        _epoch = 0;
    }    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => Math.Max(0, Math.Min(_capacity, Volatile.Read(ref _tail) - Volatile.Read(ref _head)));

    /// <summary>
    /// Whether the buffer is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Whether the buffer is at full capacity (approximate).
    /// </summary>
    public bool IsFull => CountApprox >= _capacity;    /// <summary>
    /// Enqueues a single item, overwriting the oldest if necessary.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T item)
    {
        var tail = Interlocked.Increment(ref _tail);
        var index = (int)((tail - 1) % _capacity);
        _buffer[index] = item;
        AdvanceHeadIfNeeded(tail);
        
        // Update epoch sparingly - only when tail advances significantly
        if ((tail & 0xFF) == 0) // Every 256 items
        {
            Interlocked.Increment(ref _epoch);
        }
        
        return true;
    }/// <summary>
    /// Enqueues a batch of items.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueue(ReadOnlySpan<T> batch)
    {
        int written = 0;
        foreach (var item in batch)
        {
            TryEnqueue(item);
            written++;
        }
        return written;
    }

    /// <summary>
    /// Enqueues a batch of items with optimized epoch updating.
    /// Only updates epoch once for the entire batch instead of per item.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueueBatch(ReadOnlySpan<T> batch)
    {
        if (batch.IsEmpty) return 0;
        
        // Reserve space for the entire batch atomically
        var startTail = Interlocked.Add(ref _tail, batch.Length);
        var endTail = startTail;
        
        // Write all items to their reserved positions
        for (int i = 0; i < batch.Length; i++)
        {
            var position = startTail - batch.Length + i;
            _buffer[position % _capacity] = batch[i];
        }
        
        // Advance head if needed for the entire batch
        AdvanceHeadIfNeeded(endTail);
        
        // Update epoch once for the batch
        if ((endTail & 0xFF) == 0 || ((endTail - batch.Length) & 0xFF) > (endTail & 0xFF))
        {
            Interlocked.Increment(ref _epoch);
        }
        
        return batch.Length;
    }    private void AdvanceHeadIfNeeded(long tail)
    {
        while (true)
        {
            var head = Volatile.Read(ref _head);
            if (tail - head <= _capacity)
                break;
            
            // Try to advance head atomically
            if (Interlocked.CompareExchange(ref _head, head + 1, head) == head)
            {
                // Only notify about discarded item if we successfully advanced the head
                if (_onItemDiscarded != null)
                {
                    var discardedIndex = head % _capacity;
                    var discardedItem = _buffer[discardedIndex];
                    _onItemDiscarded(discardedItem);
                }
                break;
            }
        }
    }

    /// <summary>
    /// Clears all items from the buffer.
    /// </summary>
    public void Clear()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        // Clear the array to help GC
        if (head < tail)
        {
            for (long i = head; i < tail; i++)
            {
                _buffer[i % _capacity] = default(T)!;
            }
        }
        
        // Reset pointers
        Volatile.Write(ref _head, 0);
        Volatile.Write(ref _tail, 0);
    }    /// <summary>
    /// Copies a snapshot of the buffer into <paramref name="destination"/>.
    /// </summary>
    /// <returns>Number of items copied.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Snapshot(Span<T> destination)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var length = (int)Math.Min(Math.Min(tail - head, _capacity), destination.Length);
        
        if (length <= 0) return 0;
        
        var startIndex = (int)(head % _capacity);
        
        // Check if we need to handle wrap-around
        if (startIndex + length <= _capacity)
        {
            // Single segment copy - can use fast span copy
            var source = _buffer.AsSpan(startIndex, length);
            source.CopyTo(destination);
        }
        else
        {
            // Two-segment copy for wrap-around
            var firstSegmentLength = _capacity - startIndex;
            var secondSegmentLength = length - firstSegmentLength;
            
            var firstSource = _buffer.AsSpan(startIndex, firstSegmentLength);
            var secondSource = _buffer.AsSpan(0, secondSegmentLength);
            
            firstSource.CopyTo(destination);
            secondSource.CopyTo(destination.Slice(firstSegmentLength));
        }
        
        return length;
    }/// <summary>
    /// Convenience method that allocates an array and returns an enumerable.
    /// </summary>
    public IEnumerable<T> EnumerateSnapshot()
    {
        var tmp = new T[_capacity];
        var len = Snapshot(tmp);
        var results = new List<T>(len);
        for (int i = 0; i < len; i++)
            results.Add(tmp[i]);
        return results;
    }

    /// <summary>
    /// Creates a view of the buffer contents without copying data.
    /// Uses epoch-based consistency to ensure a stable snapshot.
    /// </summary>
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
                continue;
            
            return CreateViewFromRange(head, tail, timestampSelector);
        }
        
        // Fallback: take current snapshot even if not perfectly consistent
        var currentHead = Volatile.Read(ref _head);
        var currentTail = Volatile.Read(ref _tail);
        return CreateViewFromRange(currentHead, currentTail, timestampSelector);
    }

    /// <summary>
    /// Creates a view with explicit timestamp filtering.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<T> CreateViewFiltered(
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T> timestampSelector)
    {
        if (timestampSelector == null)
            throw new ArgumentNullException(nameof(timestampSelector));
            
        const int maxRetries = 10;
        
        for (int retry = 0; retry < maxRetries; retry++)
        {
            var startEpoch = Volatile.Read(ref _epoch);
            var head = Volatile.Read(ref _head);
            var tail = Volatile.Read(ref _tail);
            var endEpoch = Volatile.Read(ref _epoch);
            
            if (startEpoch != endEpoch)
                continue;
            
            return CreateFilteredViewFromRange(head, tail, fromTicks, toTicks, timestampSelector);
        }
        
        // Fallback
        var currentHead = Volatile.Read(ref _head);
        var currentTail = Volatile.Read(ref _tail);
        return CreateFilteredViewFromRange(currentHead, currentTail, fromTicks, toTicks, timestampSelector);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateViewFromRange(long head, long tail, IEventTimestampSelector<T>? timestampSelector)
    {
        var count = (int)Math.Min(Math.Min(tail - head, _capacity), int.MaxValue);
        if (count <= 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, 0, 0);
        }
        
        var startIndex = (int)(head % _capacity);
        var endIndex = (int)((head + count - 1) % _capacity);
        
        long fromTicks = 0, toTicks = 0;
        if (timestampSelector != null && count > 0)
        {
            var firstEvent = _buffer[startIndex];
            var lastEvent = _buffer[endIndex];
            fromTicks = timestampSelector.GetTimestamp(firstEvent).Ticks;
            toTicks = timestampSelector.GetTimestamp(lastEvent).Ticks;
        }
        
        // Check if we need to wrap around
        if (startIndex + count <= _capacity)
        {
            // Single segment case
            var segment = _buffer.AsMemory(startIndex, count);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, count, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case: two segments
            var firstSegmentLength = _capacity - startIndex;
            var secondSegmentLength = count - firstSegmentLength;
            
            var segment1 = _buffer.AsMemory(startIndex, firstSegmentLength);
            var segment2 = _buffer.AsMemory(0, secondSegmentLength);
            
            return new PartitionView<T>(segment1, segment2, count, fromTicks, toTicks);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateFilteredViewFromRange(
        long head, 
        long tail, 
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T> timestampSelector)
    {
        var totalCount = (int)Math.Min(Math.Min(tail - head, _capacity), int.MaxValue);
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
            var index = (int)((head + i) % _capacity);
            var item = _buffer[index];
            var itemTicks = timestampSelector.GetTimestamp(item).Ticks;
            
            if (itemTicks >= fromTicks && itemTicks <= toTicks)
            {
                if (validStart == -1)
                    validStart = i;
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
        var actualStartIndex = (int)((head + validStart) % _capacity);
        var actualEndIndex = (int)((head + validEnd) % _capacity);
        
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
            var firstSegmentLength = _capacity - actualStartIndex;
            var secondSegmentLength = actualEndIndex + 1;
            
            var segment1 = _buffer.AsMemory(actualStartIndex, firstSegmentLength);
            var segment2 = _buffer.AsMemory(0, secondSegmentLength);
            
            return new PartitionView<T>(segment1, segment2, validCount, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Internal method for window aggregation. Enumerates items within a timestamp range
    /// without allocating collections, using a callback for each valid item.
    /// </summary>
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
        var count = Math.Min(tail - head, _capacity);
        
        for (long i = 0; i < count; i++)
        {
            var index = (head + i) % _capacity;
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void AdvanceWindowTo<TState>(
        long windowStartTicks,
        IEventTimestampSelector<T>? timestampSelector,
        ref TState state,
        WindowAdvanceCallback<T, TState> removeCallback,
        ref int windowHeadIndex)
    {
        if (timestampSelector == null) return;
        
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, _capacity);
        
        // Find new window head position
        var newWindowHead = windowHeadIndex;
        
        for (long i = windowHeadIndex; i < count; i++)
        {
            var index = (head + i) % _capacity;
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotZeroAlloc<TBuffer>(Action<ReadOnlySpan<T>> processor, ArrayPool<T> pool, int chunkSize = Buffers.DefaultChunkSize)
    {
        var buffer = pool.Rent(Math.Min(chunkSize, _capacity));
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ProcessItemsZeroAlloc<TState>(TState initialState, Func<TState, T, (TState State, bool Continue)> processor, out TState finalState)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, _capacity);
        
        var state = initialState;
        for (long i = 0; i < count; i++)
        {
            var index = (head + i) % _capacity;
            var item = _buffer[index];
            
            var result = processor(state, item);
            state = result.State;
            
            if (!result.Continue)
                break; // Early termination
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
